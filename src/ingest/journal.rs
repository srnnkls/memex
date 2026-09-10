//! macOS keeps a persistent per-volume journal of file-system events. Replaying it from the
//! event id recorded by the last committed refresh names every path that changed under the
//! source roots since then, so a refresh can skip the stat pass over every known file. Any
//! sign that the journal is incomplete (dropped events, a wrapped or purged id range, a
//! different volume) falls back to the full stamped walk.

use std::collections::HashSet;
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalCursor {
    pub device_uuid: String,
    pub event_id: u64,
}

/// Cursor to persist with the refresh that captured it, valid for one root/filter fingerprint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalCursorUpdate {
    pub fingerprint: String,
    pub cursor: JournalCursor,
}

#[derive(Debug)]
pub enum Replay {
    /// Every path that may have changed since the previous cursor.
    Changed(HashSet<PathBuf>),
    Unusable(&'static str),
}

#[derive(Debug)]
pub struct JournalReplay {
    /// Captured before any events were read, so the next replay overlaps this one.
    pub next: Option<JournalCursor>,
    pub outcome: Replay,
}

impl JournalReplay {
    fn unusable(next: Option<JournalCursor>, reason: &'static str) -> Self {
        Self {
            next,
            outcome: Replay::Unusable(reason),
        }
    }
}

pub const REPLAY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// Replay cost grows with every event the volume logged since the cursor, not only those under
/// the roots: about 150 ms per million on an M1 Pro. Beyond this distance a walk is cheaper.
pub const MAX_REPLAY_DISTANCE: u64 = 500_000;

#[cfg(not(target_os = "macos"))]
pub fn replay(
    _roots: &[PathBuf],
    _previous: Option<&JournalCursor>,
    _timeout: std::time::Duration,
) -> JournalReplay {
    JournalReplay::unusable(None, "unsupported platform")
}

#[cfg(target_os = "macos")]
pub use fsevents::replay;

#[cfg(target_os = "macos")]
mod fsevents {
    use super::{JournalCursor, JournalReplay, Replay};
    use fsevent_sys as fse;
    use fsevent_sys::core_foundation as cf;
    use std::collections::HashSet;
    use std::ffi::{CStr, CString};
    use std::os::raw::{c_char, c_void};
    use std::os::unix::fs::MetadataExt;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    type CFUUIDRef = cf::CFRef;

    #[link(name = "CoreServices", kind = "framework")]
    unsafe extern "C" {
        fn FSEventsCopyUUIDForDevice(dev: libc::dev_t) -> CFUUIDRef;
    }

    #[link(name = "CoreFoundation", kind = "framework")]
    unsafe extern "C" {
        fn CFUUIDCreateString(allocator: cf::CFAllocatorRef, uuid: CFUUIDRef) -> cf::CFStringRef;
        fn CFRunLoopRunInMode(
            mode: cf::CFStringRef,
            seconds: cf::CFTimeInterval,
            return_after_source_handled: cf::Boolean,
        ) -> i32;
    }

    const INVALIDATING: fse::FSEventStreamEventFlags = fse::kFSEventStreamEventFlagMustScanSubDirs
        | fse::kFSEventStreamEventFlagUserDropped
        | fse::kFSEventStreamEventFlagKernelDropped
        | fse::kFSEventStreamEventFlagEventIdsWrapped
        | fse::kFSEventStreamEventFlagRootChanged
        | fse::kFSEventStreamEventFlagMount
        | fse::kFSEventStreamEventFlagUnmount;

    struct Collector {
        paths: HashSet<PathBuf>,
        events: usize,
        done: bool,
        unusable: Option<&'static str>,
    }

    extern "C" fn collect(
        _stream: fse::FSEventStreamRef,
        info: *mut c_void,
        count: usize,
        paths: *mut c_void,
        flags: *const fse::FSEventStreamEventFlags,
        _ids: *const fse::FSEventStreamEventId,
    ) {
        // SAFETY: `info` is the `Collector` owned by `replay`, which outlives the stream, and the
        // arrays hold `count` entries for the duration of the callback.
        let collector = unsafe { &mut *(info as *mut Collector) };
        let paths = paths as *const *const c_char;
        for index in 0..count {
            let flags = unsafe { *flags.add(index) };
            collector.events += 1;
            if flags & fse::kFSEventStreamEventFlagHistoryDone != 0 {
                collector.done = true;
                continue;
            }
            if flags & INVALIDATING != 0 {
                collector.unusable = Some("journal incomplete");
                continue;
            }
            let directory_kept = flags & fse::kFSEventStreamEventFlagItemIsDir != 0
                && flags
                    & (fse::kFSEventStreamEventFlagItemRenamed
                        | fse::kFSEventStreamEventFlagItemRemoved)
                    == 0;
            if directory_kept {
                continue;
            }
            let path = unsafe { CStr::from_ptr(*paths.add(index)) };
            let path = String::from_utf8_lossy(path.to_bytes());
            let path = path.trim_end_matches('/');
            if !path.is_empty() {
                collector.paths.insert(PathBuf::from(path));
            }
        }
    }

    fn cf_string(value: &str) -> Option<cf::CFStringRef> {
        let value = CString::new(value).ok()?;
        let string = unsafe {
            cf::CFStringCreateWithCString(
                cf::kCFAllocatorDefault,
                value.as_ptr(),
                cf::kCFStringEncodingUTF8,
            )
        };
        (!string.is_null()).then_some(string)
    }

    fn rust_string(string: cf::CFStringRef) -> Option<String> {
        let mut buffer = vec![0u8; 128];
        let ok = unsafe {
            cf::CFStringGetCString(
                string,
                buffer.as_mut_ptr() as *mut c_char,
                buffer.len() as cf::CFIndex,
                cf::kCFStringEncodingUTF8,
            )
        };
        if !ok {
            return None;
        }
        let end = buffer.iter().position(|byte| *byte == 0)?;
        String::from_utf8(buffer[..end].to_vec()).ok()
    }

    fn device_uuid(device: libc::dev_t) -> Option<String> {
        let uuid = unsafe { FSEventsCopyUUIDForDevice(device) };
        if uuid.is_null() {
            return None;
        }
        let string = unsafe { CFUUIDCreateString(cf::kCFAllocatorDefault, uuid) };
        unsafe { cf::CFRelease(uuid) };
        if string.is_null() {
            return None;
        }
        let value = rust_string(string);
        unsafe { cf::CFRelease(string) };
        value
    }

    pub fn replay(
        roots: &[PathBuf],
        previous: Option<&JournalCursor>,
        timeout: Duration,
    ) -> JournalReplay {
        crate::profiling::span!("journal.replay");
        let mut device = None;
        let mut watched = Vec::new();
        for root in roots {
            let Ok(metadata) = std::fs::metadata(root) else {
                continue;
            };
            match device {
                None => device = Some(metadata.dev()),
                Some(seen) if seen != metadata.dev() => {
                    return JournalReplay::unusable(None, "roots span devices");
                }
                Some(_) => {}
            }
            watched.push(root.clone());
        }
        let Some(device) = device else {
            return JournalReplay::unusable(None, "no roots");
        };
        let Some(device_uuid) = device_uuid(device as libc::dev_t) else {
            return JournalReplay::unusable(None, "device without journal");
        };
        let event_id = unsafe { fse::FSEventsGetCurrentEventId() };
        let next = Some(JournalCursor {
            device_uuid: device_uuid.clone(),
            event_id,
        });
        let Some(previous) = previous else {
            return JournalReplay::unusable(next, "no cursor");
        };
        if previous.device_uuid != device_uuid {
            return JournalReplay::unusable(next, "device changed");
        }
        if previous.event_id > event_id {
            return JournalReplay::unusable(next, "cursor ahead of journal");
        }
        if event_id - previous.event_id > super::MAX_REPLAY_DISTANCE {
            return JournalReplay::unusable(next, "cursor too far behind");
        }
        if previous.event_id == event_id {
            return JournalReplay {
                next,
                outcome: Replay::Changed(HashSet::new()),
            };
        }

        let mut collector = Collector {
            paths: HashSet::new(),
            events: 0,
            done: false,
            unusable: None,
        };
        let outcome = unsafe { run_stream(&watched, previous.event_id, timeout, &mut collector) };
        crate::profiling::count!("journal.events", collector.events);
        JournalReplay {
            next,
            outcome: match outcome {
                Err(reason) => Replay::Unusable(reason),
                Ok(()) => match collector.unusable {
                    Some(reason) => Replay::Unusable(reason),
                    None if collector.done => Replay::Changed(collector.paths),
                    None => Replay::Unusable("replay timed out"),
                },
            },
        }
    }

    unsafe fn run_stream(
        roots: &[PathBuf],
        since: fse::FSEventStreamEventId,
        timeout: Duration,
        collector: &mut Collector,
    ) -> Result<(), &'static str> {
        let array = unsafe {
            cf::CFArrayCreateMutable(cf::kCFAllocatorDefault, 0, &cf::kCFTypeArrayCallBacks)
        };
        if array.is_null() {
            return Err("array allocation failed");
        }
        for root in roots {
            let Some(string) = root.to_str().and_then(cf_string) else {
                unsafe { cf::CFRelease(array) };
                return Err("root path is not UTF-8");
            };
            unsafe {
                cf::CFArrayAppendValue(array, string);
                cf::CFRelease(string);
            }
        }
        let context = fse::FSEventStreamContext {
            version: 0,
            info: collector as *mut Collector as *mut c_void,
            retain: None,
            release: None,
            copy_description: None,
        };
        let stream = unsafe {
            fse::FSEventStreamCreate(
                cf::kCFAllocatorDefault,
                collect,
                &context,
                array,
                since,
                0.0,
                fse::kFSEventStreamCreateFlagFileEvents | fse::kFSEventStreamCreateFlagNoDefer,
            )
        };
        unsafe { cf::CFRelease(array) };
        if stream.is_null() {
            return Err("stream creation failed");
        }
        let run_loop = unsafe { cf::CFRunLoopGetCurrent() };
        unsafe {
            fse::FSEventStreamScheduleWithRunLoop(stream, run_loop, cf::kCFRunLoopDefaultMode)
        };
        let started = unsafe { fse::FSEventStreamStart(stream) } != 0;
        if started {
            let deadline = Instant::now() + timeout;
            while !collector.done && collector.unusable.is_none() {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                unsafe {
                    CFRunLoopRunInMode(cf::kCFRunLoopDefaultMode, remaining.as_secs_f64(), 1)
                };
            }
            unsafe { fse::FSEventStreamStop(stream) };
        }
        unsafe {
            fse::FSEventStreamInvalidate(stream);
            fse::FSEventStreamRelease(stream);
        }
        if started {
            Ok(())
        } else {
            Err("stream did not start")
        }
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::path::Path;

    fn touch(path: &Path, text: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = fs::File::create(path).unwrap();
        file.write_all(text.as_bytes()).unwrap();
    }

    /// The journal trails the kernel by a few tens of milliseconds.
    fn settle() {
        std::thread::sleep(std::time::Duration::from_millis(150));
    }

    fn changed(replay: JournalReplay) -> HashSet<PathBuf> {
        match replay.outcome {
            Replay::Changed(paths) => paths,
            Replay::Unusable(reason) => panic!("journal unusable: {reason}"),
        }
    }

    #[test]
    fn a_replay_names_the_files_written_since_the_cursor_and_nothing_older() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap().join("sessions");
        touch(&root.join("old.jsonl"), "1");
        settle();
        let roots = vec![root.clone()];
        let first = replay(&roots, None, REPLAY_TIMEOUT);
        assert!(matches!(first.outcome, Replay::Unusable("no cursor")));
        let cursor = first.next.expect("cursor on a journaled volume");
        touch(&root.join("new.jsonl"), "2");
        touch(&root.join("deep/agent.jsonl"), "3");
        settle();
        let second = replay(&roots, Some(&cursor), REPLAY_TIMEOUT);
        let paths = changed(second);
        assert!(paths.contains(&root.join("new.jsonl")), "{paths:?}");
        assert!(paths.contains(&root.join("deep/agent.jsonl")), "{paths:?}");
        assert!(!paths.contains(&root.join("old.jsonl")), "{paths:?}");
    }

    #[test]
    fn a_foreign_or_future_cursor_is_unusable() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let roots = vec![root];
        let current = replay(&roots, None, REPLAY_TIMEOUT).next.unwrap();
        let foreign = JournalCursor {
            device_uuid: "not-this-volume".into(),
            event_id: current.event_id,
        };
        assert!(matches!(
            replay(&roots, Some(&foreign), REPLAY_TIMEOUT).outcome,
            Replay::Unusable("device changed")
        ));
        let future = JournalCursor {
            event_id: u64::MAX / 2,
            ..current.clone()
        };
        assert!(matches!(
            replay(&roots, Some(&future), REPLAY_TIMEOUT).outcome,
            Replay::Unusable("cursor ahead of journal")
        ));
        let stale = JournalCursor {
            event_id: current.event_id.saturating_sub(MAX_REPLAY_DISTANCE + 1),
            ..current
        };
        assert!(matches!(
            replay(&roots, Some(&stale), REPLAY_TIMEOUT).outcome,
            Replay::Unusable("cursor too far behind")
        ));
        assert!(matches!(
            replay(&[], None, REPLAY_TIMEOUT).outcome,
            Replay::Unusable("no roots")
        ));
    }
}
