using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.IO;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Examine.LuceneEngine.Directories;
using Examine.RemoteDirectory;
using Lucene.Net.Store;

namespace Examine.S3Directory
{
    public class S3RemoteDirectory : IRemoteDirectory
    {
        private readonly string _containerName;
        private readonly string _rootFolderName;
        private readonly AmazonS3Client _s3Client;
        public S3RemoteDirectory(BasicAWSCredentials credentials, string containerName,
            string rootFolderName)
        {
            if (credentials == null)
                throw new ArgumentNullException(nameof(credentials));
            if (string.IsNullOrWhiteSpace(containerName))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(containerName));
            _containerName = containerName;
            _s3Client = new AmazonS3Client(credentials);
            _rootFolderName = NormalizeContainerRootFolder(rootFolderName);
            EnsureContainer(containerName);
        }

        /// <summary>. </summary>
        public void SyncFile(Lucene.Net.Store.Directory directory, string fileName, bool CompressBlobs)
        {
            Trace.WriteLine($"INFO Syncing file {fileName} for {_rootFolderName}");
            // then we will get it fresh into local deflatedName 
            // StreamOutput deflatedStream = new StreamOutput(CacheDirectory.CreateOutput(deflatedName));
          



                // seek back to begininng

                if (ShouldCompressFile(fileName, CompressBlobs))
                {
                    using (var deflatedStream = new MemoryStream())
                    {
#if FULLDEBUG
                Trace.WriteLine($"GET {fileName} RETREIVED {deflatedStream.Length} bytes");
#endif
                        // get the deflated blob
                        blob.DownloadTo(deflatedStream);
                        deflatedStream.Seek(0, SeekOrigin.Begin);

                        // open output file for uncompressed contents
                        using (var fileStream = new StreamOutput(directory.CreateOutput(fileName)))
                        using (var decompressor = new DeflateStream(deflatedStream, CompressionMode.Decompress))
                        {
                            var bytes = new byte[65535];
                            var nRead = 0;
                            do
                            {
                                nRead = decompressor.Read(bytes, 0, 65535);
                                if (nRead > 0)
                                    fileStream.Write(bytes, 0, nRead);
                            } while (nRead == 65535);
                        }
                    }
                }
                else
                {
                    using (var fileStream = new StreamOutput(directory.CreateOutput(fileName)))
                    {
                        // get the blob
                        blob.DownloadTo(fileStream);

                        fileStream.Flush();
#if FULLDEBUG
                        Trace.WriteLine($"GET {fileName} RETREIVED {fileStream.Length} bytes");
#endif
                    }
                }
        }

        public long FileLength(string filename, long lenghtFallback)
        {
            try
            {
                S3FileInfo s3FileInfo = new S3FileInfo(_s3Client, _containerName, _rootFolderName + filename);
                GetObjectMetadataRequest request = new GetObjectMetadataRequest();
                request.Key = _rootFolderName + filename;
                request.BucketName = _containerName;
            
                GetObjectMetadataResponse response = _s3Client.GetObjectMetadata(request);
                var CachedLength = response.Metadata["CachedLength"];

                if (!string.IsNullOrEmpty(CachedLength) && long.TryParse(CachedLength, out var ObjectLength))
                {
                    return ObjectLength;
                }
                return s3FileInfo.Length;
            }
            catch (Exception e)
            {
                //  Sync(name);
                Trace.WriteLine(
                    $"ERROR {e.ToString()}  Exception thrown while retrieving file length of file {filename} for {_rootFolderName}");
                return lenghtFallback;
            }
        }

        public IEnumerable<string> GetAllRemoteFileNames()
        {
            ListObjectsV2Request request = new ListObjectsV2Request
            {
                BucketName = _containerName
            };
            var results = _s3Client.ListObjectsV2(request).S3Objects.Select(x => x.Key);
            return results.ToArray();
        }

        public void DeleteFile(string name)
        {
            var blob = _blobContainer.GetBlobClient(_rootFolderName + name);
            Trace.WriteLine($"INFO Deleted {_blobContainer.Uri}/{name} for {_rootFolderName}");
            Trace.WriteLine($"INFO DELETE {_blobContainer.Uri}/{name}");
            blob.DeleteIfExists();
        }

        public long FileModified(string name)
        {
            if (TryGetBlobFile(name, out var blob, out var err))
            {
                var blobPropertiesResponse = blob.GetProperties();
                var blobProperties = blobPropertiesResponse.Value;
                if (blobProperties.LastModified != null)
                {
                    var utcDate = blobProperties.LastModified.UtcDateTime;

                    //This is the data structure of how the default Lucene FSDirectory returns this value so we want
                    // to be consistent with how Lucene works
                    return (long) utcDate.Subtract(new DateTime(1970, 1, 1, 0, 0, 0)).TotalMilliseconds;
                }

                // TODO: Need to check lucene source, returning this value could be problematic
                return 0;
            }
            else
            {
                Trace.WriteLine($"WARNING Throwing exception as blob file ({name}) not found for {_rootFolderName}");
                // Lucene expects this exception to be thrown
                throw new FileNotFoundException(name, err);
            }
        }

        public bool Upload(IndexInput input, string name, long originalLength, bool CompressBlobs,
            string lastModified = null)
        {
            Stream stream;
            // optionally put a compressor around the blob stream
            if (ShouldCompressFile(name, CompressBlobs))
            {
                stream = CompressStream(input, name, originalLength);
            }
            else
            {
                stream = new StreamInput(input);
            }

            try
            {
                var fileTransferUtility =
                    new TransferUtility(_s3Client);
                var request = new TransferUtilityUploadRequest();
                request.Key = name;
                request.BucketName = _containerName;
                request.InputStream = stream;
                request.Metadata.Add("CachedLength", originalLength.ToString());
                request.Metadata.Add("CachedLastModified", lastModified);
                fileTransferUtility.Upload(request);

                // set the metadata with the original index file properties
                var metadata = new Dictionary<string, string>();
                metadata.Add("CachedLength", originalLength.ToString());
                if (!string.IsNullOrWhiteSpace(lastModified))
                {
                    metadata.Add("CachedLastModified", lastModified);
                }


#if FULLDEBUG
                Trace.WriteLine($"PUT {stream.Length} bytes to {name} in cloud");
#endif
                return true;
            }
            finally
            {
                stream.Dispose();
            }

        }

        public bool TryGetBlobFile(string name)
        {
            if (TryGetBlobFile(name, out var blop, out var err))
            {
                return true;
            }

            return false;
        }

        public bool Upload(MemoryStream stream, string fileName)
        {
            //var blob = _blobContainer.GetBlobClient(fileName);
            EnsureContainer(_containerName);
            try
            {
                var fileTransferUtility =
                    new TransferUtility(_s3Client);
                var request = new TransferUtilityUploadRequest();
                request.Key = fileName;
                request.BucketName = _containerName;
                request.InputStream = stream;
                fileTransferUtility.Upload(request);
                return true;
            }
            finally
            {
                stream.Dispose();
            }

        }

        private bool TryGetBlobFile(string name, out BlobClient blob, out RequestFailedException err)
        {
            try
            {
                blob = _blobContainer.GetBlobClient(_rootFolderName + name);
                var properties = blob.GetProperties();
                err = null;
                return true;
            }
            catch (RequestFailedException e)
            {
                Trace.WriteLine(
                    $"ERROR {e.ToString()}  Exception thrown while trying to retrieve blob ({name}). Assuming blob does not exist for {_rootFolderName}");
                err = e;
                blob = null;
                return false;
            }
        }

        protected virtual MemoryStream CompressStream(IndexInput indexInput, string fileName, long originalLength)
        {
            // unfortunately, deflate stream doesn't allow seek, and we need a seekable stream
            // to pass to the blob storage stuff, so we compress into a memory stream
            MemoryStream compressedStream = new MemoryStream();

            try
            {
                using (var compressor = new DeflateStream(compressedStream, CompressionMode.Compress, true))
                {
                    // compress to compressedOutputStream
                    byte[] bytes = new byte[indexInput.Length()];
                    indexInput.ReadBytes(bytes, 0, (int) bytes.Length);
                    compressor.Write(bytes, 0, (int) bytes.Length);
                }

                // seek back to beginning of comrpessed stream
                compressedStream.Seek(0, SeekOrigin.Begin);
#if FULLDEBUG
                Trace.WriteLine(
                    $"COMPRESSED {originalLength} -> {compressedStream.Length} {((float) compressedStream.Length / (float) originalLength) * 100}% to {fileName}");
#endif
            }
            catch
            {
                // release the compressed stream resources if an error occurs
                compressedStream.Dispose();
                throw;
            }
            finally
            {
                indexInput?.Close();
            }

            return compressedStream;
        }

        public void EnsureContainer(string containerName)
        {
            Trace.WriteLine($"DEBUG Ensuring container ({containerName}) exists");
            var blobContainer = GetBlobContainerClient(containerName);
            blobContainer.CreateIfNotExists();
            _blobContainer = blobContainer;
        }

        public BlobContainerClient GetBlobContainerClient(string containerName)
        {
            return new BlobContainerClient(_storageAccountConnectionString, containerName);
        }

        public bool ShouldCompressFile(string path, bool compressBlobs)
        {
            if (!compressBlobs)
                return false;

            var ext = Path.GetExtension(path);
            switch (ext)
            {
                case ".cfs":
                case ".fdt":
                case ".fdx":
                case ".frq":
                case ".tis":
                case ".tii":
                case ".nrm":
                case ".tvx":
                case ".tvd":
                case ".tvf":
                case ".prx":
                    return true;
                default:
                    return false;
            }
        }

        protected string NormalizeContainerRootFolder(string rootFolder)
        {
            if (string.IsNullOrEmpty(rootFolder))
                return string.Empty;
            rootFolder = rootFolder.Trim('/');
            rootFolder = rootFolder + "/";
            return rootFolder;
        }

        public bool FileExists(string filename)
        {
            try
            {
                var blob = _blobContainer.GetBlobClient(filename);
                var response = blob.Exists();
                return response.Value;
            }
            catch (Exception ex)
            {
                Trace.WriteLine(
                    $"WARNING {ex.ToString()} Exception thrown while checking blob ({filename}) exists. Assuming blob does not exist for {_rootFolderName}");
                throw;
            }
        }

        public Tuple<long, DateTime> GetFileProperties(string filename)
        {
            var blob = _blobContainer.GetBlobClient(filename);
            var blobPropertiesResponse = blob.GetProperties();
            var blobProperties = blobPropertiesResponse.Value;
            var hasMetadataValue =
                blobProperties.Metadata.TryGetValue("CachedLength", out var blobLengthMetadata);
            var blobLength = blobProperties.ContentLength;
            if (hasMetadataValue) long.TryParse(blobLengthMetadata, out blobLength);

            var blobLastModifiedUtc = blobProperties.LastModified.UtcDateTime;
            if (blobProperties.Metadata.TryGetValue("CachedLastModified", out var blobLastModifiedMetadata))
            {
                if (long.TryParse(blobLastModifiedMetadata, out var longLastModified))
                    blobLastModifiedUtc = new DateTime(longLastModified).ToUniversalTime();
            }

            return new Tuple<long, DateTime>(blobLength,blobLastModifiedUtc);
        }
    }
}