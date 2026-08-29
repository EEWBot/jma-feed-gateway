//! バックグラウンドタスクのsupervisor。
//!
//! spawnした長命タスクのpanic・異常終了を検知し、バックオフ付きで再起動する。
//! 再起動不能なタスク(aggregator)が死んだ場合はプロセス全体のシャットダウンを
//! 発火し、main が exit code 1 で終了する(Docker/systemd 側の再起動に委ねる)。
//!
//! `run_connection` / `poller::run` は既に内部で再接続・リトライループを持つため、
//! ここでのバックオフが効くのは実質 panic からの復帰時のみ。
//!
//! 不変条件:
//! - `TaskExit::Done` を返したタスクは再起動しない(意図した終了)。
//! - readiness フラグは接続/稼働の lifetime に束ねた Drop ガード
//!   (`WsConnectionGuard` / `PollActiveGuard`)が落とす。panic unwind でも
//!   future の drop でもその場でクリアされるため、ここのバックオフ
//!   (最大60秒)の間 readiness が固着することはない。

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

/// supervise対象タスクの終了理由。
#[derive(Debug)]
pub enum TaskExit {
    /// 意図した終了(シャットダウン中・設定で無効等)。再起動しない。
    Done,
    /// 異常終了。バックオフ後に再起動する。
    Failed(String),
}

/// プロセス全体のシャットダウン信号。理由を1つだけ保持する。
///
/// `watch` を使うのは、複数の待機者(axumのgraceful shutdown)へ同報しつつ、
/// 発火済み状態を後から同期的に読めるため。
#[derive(Clone)]
pub struct Shutdown {
    tx: Arc<watch::Sender<Option<String>>>,
}

impl Shutdown {
    pub fn new() -> Self {
        let (tx, _) = watch::channel(None);
        Self { tx: Arc::new(tx) }
    }

    /// 理由を添えてシャットダウンを発火する。冪等 — 最初の理由を保持する。
    pub fn trigger(&self, reason: String) {
        self.tx.send_if_modified(|slot| {
            if slot.is_some() {
                false
            } else {
                *slot = Some(reason);
                true
            }
        });
    }

    /// 発火まで待つ。既に発火済みなら即座に返る。
    pub async fn wait(&self) {
        let mut rx = self.tx.subscribe();
        // 現在値が既にSomeなら wait_for は即座に返る
        let _ = rx.wait_for(|slot| slot.is_some()).await;
    }

    /// 発火済みなら理由を返す。main の終了コード判定に使う。
    pub fn reason(&self) -> Option<String> {
        self.tx.borrow().clone()
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

/// 再起動バックオフの設定。
#[derive(Debug, Clone)]
pub struct RestartPolicy {
    pub initial: Duration,
    pub max: Duration,
    pub multiplier: f64,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(60),
            multiplier: 2.0,
        }
    }
}

impl RestartPolicy {
    /// 次のバックオフ値へ進める。
    fn advance(&self, current: Duration) -> Duration {
        std::cmp::min(current.mul_f64(self.multiplier.max(1.0)), self.max)
    }
}

/// バックオフの0〜10%のjitter。同時多発的な再起動の同期を崩す。
/// 比率にしているのは、テストがミリ秒オーダーのポリシーで回せるようにするため。
fn jitter(backoff: Duration) -> Duration {
    let span = backoff.as_millis() as u64 / 10;
    if span == 0 {
        Duration::ZERO
    } else {
        Duration::from_millis(rand::random_range(0..=span))
    }
}

/// `JoinError` から人間可読なpanicメッセージを取り出す。
fn panic_message(error: tokio::task::JoinError) -> String {
    let payload = error.into_panic();
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// 再起動可能な長命タスクをsuperviseする(ws / poller)。
///
/// `body` は再起動のたびに呼ばれるため、クロージャ内で `state` 等を clone すること。
/// `TaskExit::Done` が返るか、ランタイム停止でタスクがcancelされるまでループする。
pub fn spawn_supervised<F, Fut>(
    name: String,
    policy: RestartPolicy,
    shutdown: Shutdown,
    mut body: F,
) -> JoinHandle<()>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = TaskExit> + Send + 'static,
{
    tokio::spawn(async move {
        let mut backoff = policy.initial;
        let mut restarts = 0usize;

        loop {
            match tokio::spawn(body()).await {
                Ok(TaskExit::Done) => {
                    tracing::info!(task = %name, restarts, "supervised task finished");
                    return;
                }
                Ok(TaskExit::Failed(reason)) => {
                    tracing::warn!(task = %name, reason = %reason, restarts, "supervised task failed");
                }
                Err(e) if e.is_panic() => {
                    tracing::error!(
                        task = %name,
                        panic = %panic_message(e),
                        restarts,
                        "supervised task panicked"
                    );
                }
                Err(_) => {
                    // cancelled = ランタイム停止中。再起動しても無意味
                    tracing::info!(task = %name, "supervised task cancelled");
                    return;
                }
            }

            // シャットダウン中なら再起動しない(タスクの正常終了を待たずに落ちる場面)
            if shutdown.reason().is_some() {
                tracing::info!(task = %name, "shutdown in progress; not restarting");
                return;
            }

            restarts += 1;
            let wait = backoff + jitter(backoff);
            tracing::info!(task = %name, backoff = ?wait, restarts, "restarting supervised task");
            tokio::time::sleep(wait).await;
            backoff = policy.advance(backoff);
        }
    })
}

/// 再起動不能な単発タスクをsuperviseする(aggregator — `event_rx` がmove済みで
/// future を作り直せない)。panicでも正常終了でもシャットダウンを発火する。
///
/// aggregator の正常終了は「event channel が閉じた」= 全送信側が消えた場合のみで、
/// これも実質的に復旧不能な状態のためシャットダウン扱いとする。
pub fn spawn_critical<Fut>(name: String, shutdown: Shutdown, body: Fut) -> JoinHandle<()>
where
    Fut: Future<Output = ()> + Send + 'static,
{
    tokio::spawn(async move {
        match tokio::spawn(body).await {
            Ok(()) => {
                tracing::error!(task = %name, "critical task exited; shutting down");
                shutdown.trigger(format!("critical task `{name}` exited"));
            }
            Err(e) if e.is_panic() => {
                let message = panic_message(e);
                tracing::error!(task = %name, panic = %message, "critical task panicked; shutting down");
                shutdown.trigger(format!("critical task `{name}` panicked: {message}"));
            }
            Err(_) => {
                tracing::info!(task = %name, "critical task cancelled");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// テスト用の高速ポリシー(実時間を待たない)。
    fn fast_policy() -> RestartPolicy {
        RestartPolicy {
            initial: Duration::from_millis(1),
            max: Duration::from_millis(5),
            multiplier: 2.0,
        }
    }

    #[tokio::test]
    async fn panicking_task_is_restarted_until_it_finishes() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();

        let handle = spawn_supervised(
            "panicky".into(),
            fast_policy(),
            Shutdown::new(),
            move || {
                let n = counter.fetch_add(1, Ordering::Relaxed);
                async move {
                    if n < 2 {
                        panic!("injected panic #{n}");
                    }
                    TaskExit::Done
                }
            },
        );

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("supervisor must converge")
            .expect("supervisor task itself must not panic");
        // 2回panic → 3回目で Done
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn done_task_is_not_restarted() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();

        let handle = spawn_supervised("once".into(), fast_policy(), Shutdown::new(), move || {
            counter.fetch_add(1, Ordering::Relaxed);
            async move { TaskExit::Done }
        });

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("supervisor must return")
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn failed_task_is_restarted() {
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();

        let handle = spawn_supervised("flaky".into(), fast_policy(), Shutdown::new(), move || {
            let n = counter.fetch_add(1, Ordering::Relaxed);
            async move {
                if n < 2 {
                    TaskExit::Failed(format!("attempt {n} failed"))
                } else {
                    TaskExit::Done
                }
            }
        });

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("supervisor must converge")
            .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn supervisor_stops_restarting_once_shutdown_is_triggered() {
        let shutdown = Shutdown::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let trigger = shutdown.clone();

        let handle = spawn_supervised(
            "stopping".into(),
            fast_policy(),
            shutdown.clone(),
            move || {
                counter.fetch_add(1, Ordering::Relaxed);
                let trigger = trigger.clone();
                async move {
                    trigger.trigger("test shutdown".into());
                    TaskExit::Failed("boom".into())
                }
            },
        );

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("supervisor must stop")
            .unwrap();
        // Failed でも shutdown 発火済みなら再起動しない
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn critical_task_panic_triggers_shutdown() {
        let shutdown = Shutdown::new();
        let handle = spawn_critical("aggregator".into(), shutdown.clone(), async {
            panic!("aggregator died");
        });
        handle.await.unwrap();

        let reason = shutdown.reason().expect("shutdown must be triggered");
        assert!(reason.contains("aggregator"), "reason was: {reason}");
        assert!(reason.contains("aggregator died"), "reason was: {reason}");
    }

    #[tokio::test]
    async fn critical_task_normal_exit_triggers_shutdown() {
        let shutdown = Shutdown::new();
        spawn_critical("aggregator".into(), shutdown.clone(), async {})
            .await
            .unwrap();
        assert!(shutdown.reason().is_some());
    }

    #[tokio::test]
    async fn shutdown_wait_resolves_and_keeps_first_reason() {
        let shutdown = Shutdown::new();
        assert!(shutdown.reason().is_none());

        shutdown.trigger("first".into());
        shutdown.trigger("second".into());

        tokio::time::timeout(Duration::from_secs(1), shutdown.wait())
            .await
            .expect("wait must resolve after trigger");
        assert_eq!(shutdown.reason().as_deref(), Some("first"));
    }

    #[test]
    fn backoff_is_capped_at_max() {
        let policy = fast_policy();
        let mut backoff = policy.initial;
        for _ in 0..10 {
            backoff = policy.advance(backoff);
        }
        assert_eq!(backoff, policy.max);
    }
}
