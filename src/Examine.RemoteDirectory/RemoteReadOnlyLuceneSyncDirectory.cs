using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Examine.Logging;
using Lucene.Net.Store;
using Directory = System.IO.Directory;

namespace Examine.RemoteDirectory
{
    public class RemoteReadOnlyLuceneSyncDirectory : RemoteSyncDirectory
    {
        private readonly string _cacheDirectoryPath;
        private readonly string _cacheDirectoryName;
        private BlockingCollectionQueue _rebuildQueue;
        private string _oldIndexFolderName;
        private bool isBooting = true;

        public RemoteReadOnlyLuceneSyncDirectory(IRemoteDirectory remoteDirectory,
            string cacheDirectoryPath,
            string cacheDirectoryName,
            ILoggingService loggingService,
            bool compressBlobs = false) : base(remoteDirectory, loggingService, compressBlobs)
        {
            _cacheDirectoryPath = cacheDirectoryPath;
            _cacheDirectoryName = cacheDirectoryName;
            _rebuildQueue = new BlockingCollectionQueue(loggingService);
            IsReadOnly = true;
            if (CacheDirectory == null)
            {
                LoggingService.Log(new LogEntry(LogLevel.Info, null,
                    $"CacheDirectory null. Creating or rebuilding cache"));

                CreateOrReadCache();
            }
            else
            {
                  CheckDirty();
            }
        }

        public override string[] CheckDirtyWithoutWriter()
        {
            return CheckDirty();
        }

        /// <summary>
        /// Checks dirty flag and sets the _inSync flag after querying the blob strorage vs local storage segment gen
        /// </summary>
        /// <returns>
        /// If _dirty is true and blob storage files are looked up, this will return those blob storage files, this is a performance gain so
        /// we don't double query blob storage.
        /// </returns>
        public override string[] CheckDirty()
        {
            SetDirty();
            //that is not best way of doing that shit, but good enough for testing
            if (NextCheck == null || DateTime.Now > NextCheck.Add(TimeSpan.FromMinutes(1)))
            {
                DirtyStrings = HandleCheckDirty();

                NextCheck = DateTime.Now;
            }
            else
            {
                LoggingService.Log(new LogEntry(LogLevel.Info, null,
                    $"Skip Checking synchronization for {RootFolder}"));

                unSetDirty();
            }

            return DirtyStrings;
        }

        public DateTime NextCheck;
        public string[] DirtyStrings;

        public RemoteReadOnlyLuceneSyncDirectory(IRemoteDirectory remoteDirectory,
            string cacheDirectoryPath,
            string cacheDirectoryName,
            bool compressBlobs = false) : base(remoteDirectory, compressBlobs)
        {
            _cacheDirectoryPath = cacheDirectoryPath;
            _cacheDirectoryName = cacheDirectoryName;
            IsReadOnly = true;
            if (CacheDirectory == null)
            {
                LoggingService.Log(new LogEntry(LogLevel.Error, null,
                    $"CacheDirectory null. Creating or rebuilding cache"));

                CreateOrReadCache();
            }
            else
            {
                LoggingService.Log(new LogEntry(LogLevel.Info, null,
                    $"Checking sync fo CacheDirectory which was passed in constructor"));

                  CheckDirty();
            }
        }

        protected override void GuardCacheDirectory(Lucene.Net.Store.Directory cacheDirectory)
        {
            //Do nothing
        }

        private void CreateOrReadCache()
        {
            LoggingService.Log(new LogEntry(LogLevel.Info, null,
                $"Called method CreateOrReadCache"));
            lock (RebuildLock)
            {
                var indexParentFolder = new DirectoryInfo(
                    Path.Combine(_cacheDirectoryPath,
                        _cacheDirectoryName));
                if (indexParentFolder.Exists)
                {
                    var subDirectories = indexParentFolder.GetDirectories();
                    if (subDirectories.Any())
                    {
                        LoggingService.Log(new LogEntry(LogLevel.Info, null,
                            $"Reopening latest index"));
                        var directory = subDirectories.LastOrDefault();
                        _oldIndexFolderName = directory.Name;
                        CacheDirectory = new SimpleFSDirectory(directory);
                        _lockFactory = CacheDirectory.LockFactory;
                    }
                    else
                    {
                        lock (RebuildLock)
                        {
                            LoggingService.Log(new LogEntry(LogLevel.Info, null,
                                $"Create task"));
                            var cache = RebuildCache();
                            LoggingService.Log(new LogEntry(LogLevel.Info, null,
                                $"Run task"));
                            cache.RunSynchronously();
                        }
                    }
                }
                else
                {
                    lock (RebuildLock)
                    {
                        LoggingService.Log(new LogEntry(LogLevel.Info, null,
                            $"Create task"));
                        var cache = RebuildCache();
                        LoggingService.Log(new LogEntry(LogLevel.Info, null,
                            $"Run task"));
                        cache.RunSynchronously();
                    }
                }
            }
        }

        public override string[] ListAll()
        {
            CheckDirty();

            return CacheDirectory.ListAll();
        }

        public override IndexOutput CreateOutput(string name)
        {
            CheckDirty();
            LoggingService.Log(new LogEntry(LogLevel.Info, null, $"Opening output for {_oldIndexFolderName}"));
            return CacheDirectory.CreateOutput(name);
        }

        public override IndexInput OpenInput(string name)
        {
            CheckDirty();
            LoggingService.Log(new LogEntry(LogLevel.Info, null, $"Opening input for {_oldIndexFolderName}"));
            return CacheDirectory.OpenInput(name);
        }

        protected override void HandleOutOfSync()

        {
            LoggingService.Log(new LogEntry(LogLevel.Info, null, $"Called HandleOutOfSync"));

            lock (RebuildLock)
            {
                _rebuildQueue.Enqueue(() => RebuildCache(true));
            }
        }

        //todo: make that as background task. Need input from someone how to handle that correctly as now it is as sync task to avoid issues, but need be change
        protected Task RebuildCache(bool handle = false)
        {
            return new Task(() =>
            {
                lock (_locker)
                {
                    lock (RebuildLock)
                    {
                        //Needs locking
                        LoggingService.Log(new LogEntry(LogLevel.Info, null, $"Rebuilding cache"));

                        var tempDir = new DirectoryInfo(
                            Path.Combine(_cacheDirectoryPath,
                                _cacheDirectoryName, DateTimeOffset.UtcNow.ToString("yyyyMMddTHHmmssfffffff")));
                        if (tempDir.Exists == false)
                            tempDir.Create();
                        if (_oldIndexFolderName != null)
                        {
                            var old = new DirectoryInfo(
                                Path.Combine(_cacheDirectoryPath,
                                    _cacheDirectoryName, _oldIndexFolderName));
                            if (old.Exists)
                            {
                                foreach (var file in Directory.GetFiles(old.FullName))
                                {
                                    if (file.EndsWith(".lock"))
                                    {
                                        continue;
                                    }

                                    File.Copy(file, Path.Combine(tempDir.FullName, Path.GetFileName(file)));
                                }
                            }
                        }

                        Lucene.Net.Store.Directory newIndex = new SimpleFSDirectory(tempDir);
                        LoggingService.Log(new LogEntry(LogLevel.Info, null,
                            $"Getting files from blob{DateTime.Now.ToString()}"));

                        foreach (string file in GetAllBlobFiles())
                        {
                            var systemFile = new FileInfo(tempDir.FullName + "/" + file);
                            if (systemFile.Exists && RemoteDirectory.FileModified(file) >
                                CacheDirectory.FileModified(file) + 120000)
                            {
                                LoggingService.Log(new LogEntry(LogLevel.Info, null,
                                    $"File skipped as exists in previous cache"));

                                continue;
                            }

                            //   newIndex.TouchFile(file);
                            if (file.EndsWith(".lock"))
                            {
                                continue;
                            }

                            var status = RemoteDirectory.SyncFile(newIndex, file, CompressBlobs);
                            if (!status)
                            {
                                LoggingService.Log(new LogEntry(LogLevel.Error, null, $"Rebuilding cache failed"));
                                newIndex.Dispose();
                                handle = false;
                                return;
                            }
                        }

                        LoggingService.Log(new LogEntry(LogLevel.Info, null,
                            $"Getting files from blob{DateTime.Now.ToString()}"));

                        var oldIndex = CacheDirectory;
                        newIndex.Dispose();
                        newIndex = new SimpleFSDirectory(tempDir);

                        CacheDirectory = newIndex;
                        _lockFactory = newIndex.LockFactory;

                        _oldIndexFolderName = tempDir.Name;
                        if (handle)
                        {
                            HandleOutOfSyncDirectory();
                        }
                    }
                }
            });
        }

        public override bool FileExists(string name)
        {
            CheckDirty();
            return CacheDirectory.FileExists(name);
        }

        internal override string[] GetAllBlobFiles()
        {
            lock (RebuildLock)
            {
                return base.GetAllBlobFiles();
            }
        }

        public override long FileModified(string name)
        {
            CheckDirty();
            return CacheDirectory.FileModified(name);
        }

        public override void DeleteFile(string name)
        {
            CheckDirty();
            //swallow operation
        }

        public override long FileLength(string name)
        {
            CheckDirty();
            return CacheDirectory.FileLength(name);
        }

        public class BlockingCollectionQueue
        {
            private readonly ILoggingService _loggingService;
            private BlockingCollection<Func<Task>> _jobs = new BlockingCollection<Func<Task>>();
            private bool _delegateQueuedOrRunning = false;

            public BlockingCollectionQueue(ILoggingService loggingService)
            {
                _loggingService = loggingService;
                var thread = new Thread(new ThreadStart(OnStart));
                thread.IsBackground = true;
                _loggingService.Log(new LogEntry(LogLevel.Info, null, $"Background queue started"));

                thread.Start();
            }

            public void Enqueue(Func<Task> job)
            {
                _jobs.Add(job);
            }

            private void OnStart()
            {
                foreach (var job in _jobs.GetConsumingEnumerable(CancellationToken.None))
                {
                    _loggingService.Log(new LogEntry(LogLevel.Info, null,
                        $"Running background task, rebuild cache started"));

                    job.Invoke().Start();
                    _loggingService.Log(new LogEntry(LogLevel.Info, null,
                        $"Running background task, rebuild cache finished"));
                }
            }
        }
    }
}