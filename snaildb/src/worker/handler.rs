use std::sync::mpsc;
use std::thread;
use std::time::Duration;

// worker manager, manages the mpsc channel across wal, compaction, etc..
#[derive(Debug)]
pub struct WorkerManager<C> {
    /// The sender to send commands to the worker.
    pub sender: mpsc::Sender<C>,
    /// The thread handle to join the thread.
    _thread_handle: thread::JoinHandle<()>, // thread handle to join the thread
}

impl<C> WorkerManager<C> {
    /// Sends a command to the worker.
    pub fn send(&self, cmd: C) -> Result<(), mpsc::SendError<C>> {
        self.sender.send(cmd)
    }

    /// Spawns a new worker thread with the given handler and timeout.
    pub fn spawn<F>(handler: F, timeout: Duration) -> Self
    where
        F: FnOnce(mpsc::Receiver<C>, Duration) + Send + 'static, // custom function to handle the messages 
        C: Send + 'static, // command type to send
        {
            let (sender, receiver) = mpsc::channel();
            let handle = thread::spawn(move || {
                handler(receiver, timeout); // call the custom function with the receiver and timeout
            });
            Self { sender, _thread_handle: handle }
        }
}