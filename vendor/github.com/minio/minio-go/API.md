# Golang Client API Reference
Initialize Minio Client object.
### 1. Minio

```go
package main

import (
	"fmt"

	"github.com/minio/minio-go"
)

func main() {
        // Use a secure connection.
        ssl := true

        // Initialize minio client object.
        minioClient, err := minio.New("play.minio.io:9000", "Q3AM3UQ867SPQQA43P2F", "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG", ssl)
        if err != nil {
                fmt.Println(err)
          	    return
        }
}
```
### 2. AWS S3

```go
package main

import (
	"fmt"

	"github.com/minio/minio-go"
)

func main() {
        // Use a secure connection.
        ssl := true

        // Initialize minio client object.
        s3Client, err := minio.New("s3.amazonaws.com", "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", ssl)
        if err != nil {
                fmt.Println(err)
                return            
        }
}
```
| Bucket operations  |Object operations   | Presigned operations  | Bucket Policy Operations |
|:---|:---|:---|:---|
|[`MakeBucket`](#MakeBucket)   |[`GetObject`](#GetObject)   | [`PresignedGetObject`](#PresignedGetObject)  |[`SetBucketPolicy`](#SetBucketPolicy)   |
|[`ListBuckets`](#ListBuckets)   |[`PutObject`](#PutObject)   |[`PresignedPutObject`](#PresignedPutObject)   | [`GetBucketPolicy`](#GetBucketPolicy)  |
|[`BucketExists`](#BucketExists)   |[`CopyObject`](#CopyObject)   |[`PresignedPostPolicy`](#PresignedPostPolicy)   |   |
| [`RemoveBucket`](#RemoveBucket)  |[`StatObject`](#StatObject)   |   |   |
|[`ListObjects`](#ListObjects)   |[`RemoveObject`](#RemoveObject)   |   |   |
|[`ListObjectsV2`](#ListObjectsV2) | [`RemoveIncompleteUpload`](#RemoveIncompleteUpload)  |   |   |
|[`ListIncompleteUploads`](#ListIncompleteUploads) |[`FPutObject`](#FPutObject)   |   |   |
|   | [`FGetObject`](#FGetObject)  |   |   |

## 1. Constructor
<a name="Minio">
#### New(endpoint string, accessKeyID string, secretAccessKey string, ssl bool) (*Client, error)
Initializes a new client object.

__Parameters__

|Param   |Type   |Description   |
|:---|:---| :---|
|`endpoint`   | _string_  |S3 object storage endpoint.   |
| `accessKeyID`  |_string_   | Access key for the object storage endpoint.  |
| `secretAccessKey`  | _string_  |Secret key for the object storage endpoint.   |
|`ssl`   | _bool_  | Set this value to 'true' to enable secure (HTTPS) access.  |

## 2. Bucket operations

<a name="MakeBucket">
#### MakeBucket(bucketName string, location string) error
Creates a new bucket.

__Parameters__

<table>
    <thead>
        <tr>
            <th>Param</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
       <tr>
       <td>bucketName</td>
       <td>string </td>
       <td>name of the bucket</td>
       </tr>
        <tr>
            <td>
           location
            </td>
            <td> string</td>
            <td> Default value is _us-east-1_ <br/>

Region valid values are: [ _us-west-1_, _us-west-2_,  _eu-west-1_, _eu-central-1_, _ap-southeast-1_, _ap-northeast-1_, _ap-southeast-2_, _sa-east-1_ ].
            </td>
            </tr>
               </tbody>
</table>

__Example__

```go
err := minioClient.MakeBucket("mybucket", "us-east-1")
if err != nil {
    fmt.Println(err)
    return
}
fmt.Println("Successfully created mybucket.")
```

<a name="ListBuckets">
#### ListBuckets() ([]BucketInfo, error)
Lists all buckets.

<table>
    <thead>
        <tr>
            <th>Param</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
           bucketList
            </td>
            <td> []BucketInfo </td>
            <td>
            <ul>Lists bucket in following format:
            <li>bucket.Name string: bucket name.</li>
            <li>bucket.CreationDate time.Time : date when bucket was created.</li>
            </ul>
            </td>
            </tr>
               </tbody>
</table>

 __Example__
 
 ```go
 buckets, err := minioClient.ListBuckets()
if err != nil {
    fmt.Println(err)
    return
}
for _, bucket := range buckets {
  fmt.Println(bucket)
}  
 ```

<a name="BucketExists">
#### BucketExists(bucketName string) error
Checks if a bucket exists.

__Parameters__

|Param   |Type   |Description   |
|:---|:---| :---|
|`bucketName`  | _string_  |name of the bucket.   |

__Example__

```go
err := minioClient.BucketExists("mybucket")
if err != nil {
    fmt.Println(err)
    return
}
```

<a name="RemoveBucket">
#### RemoveBucket(bucketName string)
Removes a bucket.

__Parameters__

|Param   |Type   |Description   |
|:---|:---| :---|
|`bucketName`  | _string_  |name of the bucket.   |

__Example__

```go
err := minioClient.RemoveBucket("mybucket")
if err != nil {
    fmt.Println(err)
    return
}
```

<a name="ListObjects">
#### ListObjects(bucketName string, prefix string, recursive bool, doneCh chan struct{}) <-chan ObjectInfo
Lists objects in a bucket.

__Parameters__

|Param   |Type   |Description   |
|:---|:---| :---|
|`bucketName`  | _string_  |name of the bucket.   |
| `objectPrefix` |_string_   | the prefix of the objects that should be listed. |
| `recursive`  | _bool_  |`true` indicates recursive style listing and `false` indicates directory style listing delimited by '/'.  |
|`doneCh`  | _chan struct{}_ | Set this value to 'true' to enable secure (HTTPS) access.  |

__Return Value__

<table>
    <thead>
        <tr>
            <th>Param</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
           chan ObjectInfo
            </td>
            <td> chan ObjectInfo </td>
            <td>
            <ul>Read channel for all the objects in the bucket, the object is of the format:
            <li>objectInfo.Key string: name of the object.</li>
            <li>objectInfo.Size int64: size of the object.</li>
            <li>objectInfo.ETag string: etag of the object. </li>
            <li>objectInfo.LastModified time.Time: modified time stamp.</li>
            </ul>
            </td>
            </tr>
               </tbody>
</table>

```go
// Create a done channel to control 'ListObjects' go routine.
doneCh := make(chan struct{})

// Indicate to our routine to exit cleanly upon return.
defer close(doneCh)

isRecursive := true
objectCh := minioClient.ListObjects("mybucket", "myprefix", isRecursive, doneCh)
for object := range objectCh {
    if object.Err != nil {
        fmt.Println(object.Err)
        return
    }
    fmt.Println(object)
}
```

<a name="ListObjectsV2">
#### ListObjectsV2(bucketName string, prefix string, recursive bool, doneCh chan struct{}) <-chan ObjectInfo
Lists objects in a bucket using the recommanded listing API v2 

__Parameters__

|Param   |Type   |Description   |
|:---|:---| :---|
|`bucketName`  | _string_  |name of the bucket.   |
| `objectPrefix` |_string_   | the prefix of the objects that should be listed. |
| `recursive`  | _bool_  |`true` indicates recursive style listing and `false` indicates directory style listing delimited by '/'.  |
|`doneCh`  | _chan struct{}_ | Set this value to 'true' to enable secure (HTTPS) access.  |

__Return Value__

<table>
    <thead>
        <tr>
            <th>Param</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
           chan ObjectInfo
            </td>
            <td> chan ObjectInfo </td>
            <td>
            <ul>Read channel for all the objects in the bucket, the object is of the format:
            <li>objectInfo.Key string: name of the object.</li>
            <li>objectInfo.Size int64: size of the object.</li>
            <li>objectInfo.ETag string: etag of the object. </li>
            <li>objectInfo.LastModified time.Time: modified time stamp.</li>
            </ul>
            </td>
            </tr>
               </tbody>
</table>

```go
// Create a done channel to control 'ListObjectsV2' go routine.
doneCh := make(chan struct{})

// Indicate to our routine to exit cleanly upon return.
defer close(doneCh)

isRecursive := true
objectCh := minioClient.ListObjectsV2("mybucket", "myprefix", isRecursive, doneCh)
for object := range objectCh {
    if object.Err != nil {
        fmt.Println(object.Err)
        return
    }
    fmt.Println(object)
}
```



<a name="ListIncompleteUploads">
#### ListIncompleteUploads(bucketName string, prefix string, recursive bool, doneCh chan struct{}) <- chan ObjectMultipartInfo
Lists partially uploaded objects in a bucket.

__Parameters__

|Param   |Type   |Description   |
|:---|:---| :---|
|`bucketName`  | _string_  |name of the bucket.   |
| `prefix` |_string_   | prefix of the object names that are partially uploaded |
| `recursive`  | _bool_  |`true` indicates recursive style listing and `false` indicates directory style listing delimited by '/'.  |
|`doneCh`  | _chan struct{}_ | Set this value to 'true' to enable secure (HTTPS) access.  |

__Return Value__

<table>
    <thead>
        <tr>
            <th>Param</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
           chan ObjectMultipartInfo
            </td>
            <td> chan ObjectMultipartInfo </td>
            <td>
            <ul>emits multipart objects of the format:
            <li>multiPartObjInfo.Key string: name of the incomplete object.</li>
            <li>multiPartObjInfo.UploadID string: upload ID of the incomplete object.</li>
            <li>multiPartObjInfo.Size int64: size of the incompletely uploaded object.</li>
            </ul>
            </td>
            </tr>
               </tbody>
</table>

__Example__

```go
// Create a done channel to control 'ListObjects' go routine.
doneCh := make(chan struct{})

// Indicate to our routine to exit cleanly upon return.
defer close(doneCh)

isRecursive := true // Recursively list everything at 'myprefix'
multiPartObjectCh := minioClient.ListIncompleteUploads("mybucket", "myprefix", isRecursive, doneCh)
for multiPartObject := range multiPartObjectCh {
    if multiPartObject.Err != nil {
        fmt.Println(multiPartObject.Err)
        return
    }
    fmt.Println(multiPartObject)
}
```

## 3. Object operations
<a name="GetObject">
#### GetObject(bucketName string, objectName string) (*Object, error)
Downloads an object.

__Parameters__

|Param   |Type   |Description   |
|:---|:---| :---|
|`bucketName`  | _string_  |name of the bucket.   |
|`objectName` | _string_  |name of the object.   |

__Return Value__

|Param   |Type   |Description   |
|:---|:---| :---|
|`object`  | _*minio.Object_ |_minio.Object_ represents object reader  |

__Example__

```go
object, err := minioClient.GetObject("mybucket", "photo.jpg")
if err != nil {
    fmt.Println(err)
    return
}
localFile, err := os.Create("/tmp/local-file.jpg")
if err != nil {
    fmt.Println(err)
    return
}
if _, err = io.Copy(localFile, object); err != nil {
    fmt.Println(err)
    return
}
```
<a name="FGetObject">
#### FGetObject(bucketName string, objectName string, filePath string) error
 Downloads and saves the object as a file in the local filesystem.

__Parameters__


|Param   |Type   |Description   |
|:---|:---| :---|
|`bucketName`  | _string_  |name of the bucket.   |
|`objectName` | _string_  |name of the object.   |
|`filePath` | _string_  |path to which the object data will be written to.   |

__Example__

```go
err := minioClient.FGetObject("mybucket", "photo.jpg", "/tmp/photo.jpg")
if err != nil {
    fmt.Println(err)
    return
}
```

<a name="PutObject">
#### PutObject(bucketName string, objectName string, reader io.Reader, contentType string) (n int, err error) 
Uploads an object.

__Parameters__

|Param   |Type   |Description   |
|:---|:---| :---|
|`bucketName`  | _string_  |name of the bucket.   |
|`objectName` | _string_  |name of the object.   |
|`reader` | _io.Reader_  |Any golang object implementing io.Reader.   |
|`contentType` | _string_  |content type of the object.  |

__Example__

Uploads objects that are less than 5MiB in a single PUT operation. For objects that are greater than the 5MiB in size, PutObject seamlessly uploads the object in chunks of 5MiB or more depending on the actual file size. The max upload size for an object is 5TB.

In the event that PutObject fails to upload an object, the user may attempt to re-upload the same object. If the same object is being uploaded, PutObject API examines the previous partial attempt to upload this object and resumes automatically from where it left off.

```go
file, err := os.Open("my-testfile")
if err != nil {
    fmt.Println(err)
    return
}
defer file.Close()

n, err := minioClient.PutObject("mybucket", "myobject", file, "application/octet-stream")
if err != nil {
    fmt.Println(err)
    return
}
```

<a name="CopyObject">
#### CopyObject(bucketName string, objectName string, objectSource string, conditions CopyConditions) error
Copy a source object into a new object with the provided name in the provided bucket.

__Parameters__

|Param   |Type   |Description   |
|:---|:---| :---|
|`bucketName`  | _string_  |name of the bucket.   |
|`objectName` | _string_  |name of the object.   |
|`objectSource` | _string_  |name of the object source.  |
|`conditions` | _CopyConditions_  |Collection of supported CopyObject conditions. ['x-amz-copy-source', 'x-amz-copy-source-if-match', 'x-amz-copy-source-if-none-match', 'x-amz-copy-source-if-unmodified-since', 'x-amz-copy-source-if-modified-since'].|

__Example__

```go
// All following conditions are allowed and can be combined together.

// Set copy conditions.
var copyConds = minio.NewCopyConditions()
// Set modified condition, copy object modified since 2014 April.
copyConds.SetModified(time.Date(2014, time.April, 0, 0, 0, 0, 0, time.UTC))

// Set unmodified condition, copy object unmodified since 2014 April.
// copyConds.SetUnmodified(time.Date(2014, time.April, 0, 0, 0, 0, 0, time.UTC))

// Set matching ETag condition, copy object which matches the following ETag.
// copyConds.SetMatchETag("31624deb84149d2f8ef9c385918b653a")

// Set matching ETag except condition, copy object which does not match the following ETag.
// copyConds.SetMatchETagExcept("31624deb84149d2f8ef9c385918b653a")

err := minioClient.CopyObject("mybucket", "myobject", "/my-sourcebucketname/my-sourceobjectname", copyConds)
if err != nil {
    fmt.Println(err)
    return
}
```

<a name="FPutObject">
#### FPutObject(bucketName string, objectName string, filePath string, contentType string) error
Uploads contents from a file to objectName. 

__Parameters__

|Param   |Type   |Description   |
|:---|:---| :---|
|`bucketName`  | _string_  |name of the bucket.   |
|`objectName` | _string_  |name of the object.   |
|`filePath` | _string_  |file path of the file to be uploaded. |
|`contentType` | _string_  |content type of the object.  |

__Example__

FPutObject uploads objects that are less than 5MiB in a single PUT operation. For objects that are greater than the 5MiB in size, FPutObject seamlessly uploads the object in chunks of 5MiB or more depending on the actual file size. The max upload size for an object is 5TB.

In the event that FPutObject fails to upload an object, the user may attempt to re-upload the same object. If the same object is being uploaded, FPutObject API examines the previous partial attempt to upload this object and resumes automatically from where it left off.

```go
n, err := minioClient.FPutObject("mybucket", "myobject.csv", "/tmp/otherobject.csv", "application/csv")
if err != nil {
    fmt.Println(err)
    return
}
```

<a name="StatObject">
#### StatObject(bucketName string, objectName string) (ObjectInfo, error)
Gets metadata of an object.

__Parameters__

|Param   |Type   |Description   |
|:---|:---| :---|
|`bucketName`  | _string_  |name of the bucket.   |
|`objectName` | _string_  |name of the object.   |

__Return Value__

<table>
    <thead>
        <tr>
            <th>Param</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>
           objInfo
            </td>
            <td> ObjectInfo</td>
            <td>
            <ul>object stat info for following format:
            <li>objInfo.Size int64: size of the object.</li>
            <li>objInfo.ETag string: etag of the object.</li>
            <li>objInfo.ContentType string: Content-Type of the object.</li>
            <li>objInfo.LastModified time.Time: modified time stamp</li>
            </ul>
            </td>
            </tr>
               </tbody>
</table>

  __Example__
  
```go
objInfo, err := minioClient.StatObject("mybucket", "photo.jpg")
if err != nil {
    fmt.Println(err)
    return
}
fmt.Println(objInfo)
```

<a name="RemoveObject">
#### RemoveObject(bucketName string, objectName string) error
Removes an object.

__Parameters__

|Param   |Type   |Description   |
|:---|:---| :---|
|`bucketName`  | _string_  |name of the bucket.   |
|`objectName` | _string_  |name of the object.   |

```go
err := minioClient.RemoveObject("mybucket", "photo.jpg")
if err != nil {
    fmt.Println(err)
    return
}
```

<a name="RemoveIncompleteUpload">
#### RemoveIncompleteUpload(bucketName string, objectName string) error
Removes a partially uploaded object.

__Parameters__

|Param   |Type   |Description   |
|:---|:---| :---|
|`bucketName`  | _string_  |name of the bucket.   |
|`objectName` | _string_  |name of the object.   |

__Example__

```go
err := minioClient.RemoveIncompleteUpload("mybucket", "photo.jpg")
if err != nil {
    fmt.Println(err)
    return
}
```

## 4. Presigned operations

<a name="PresignedGetObject">
#### PresignedGetObject(bucketName string, objectName string, expiry time.Duration, reqParams url.Values) (*url.URL, error)
Generates a presigned URL for HTTP GET operations. Browsers/Mobile clients may point to this URL to directly download objects even if the bucket is private. This presigned URL can have an associated expiration time in seconds after which it is no longer operational. The default expiry is set to 7 days.

__Parameters__

|Param   |Type   |Description   |
|:---|:---| :---|
|`bucketName`  | _string_  |name of the bucket.   |
|`objectName` | _string_  |name of the object.   |
|`expiry` | _time.Duration_  |expiry in seconds.   |
|`reqParams` | _url.Values_  |additional response header overrides supports _response-expires_, _response-content-type_, _response-cache-control_, _response-content-disposition_.  |

__Example__

```go
// Set request parameters for content-disposition.
reqParams := make(url.Values)
reqParams.Set("response-content-disposition", "attachment; filename=\"your-filename.txt\"")

// Generates a presigned url which expires in a day.
presignedURL, err := minioClient.PresignedGetObject("mybucket", "myobject", time.Second * 24 * 60 * 60, reqParams)
if err != nil {
    fmt.Println(err)
    return
}
```

<a name="PresignedPutObject">
#### PresignedPutObject(bucketName string, objectName string, expiry time.Duration) (*url.URL, error)
Generates a presigned URL for HTTP PUT operations. Browsers/Mobile clients may point to this URL to upload objects directly to a bucket even if it is private. This presigned URL can have an associated expiration time in seconds after which it is no longer operational. The default expiry is set to 7 days.

NOTE: you can upload to S3 only with specified object name.
 

__Parameters__

|Param   |Type   |Description   |
|:---|:---| :---|
|`bucketName`  | _string_  |name of the bucket.   |
|`objectName` | _string_  |name of the object.   |
|`expiry` | _time.Duration_  |expiry in seconds.   |

__Example__

```go
// Generates a url which expires in a day.
expiry := time.Second * 24 * 60 * 60 // 1 day.
presignedURL, err := minioClient.PresignedPutObject("mybucket", "myobject", expiry)
if err != nil {
    fmt.Println(err)
    return
}
    fmt.Println(presignedURL)
```

<a name="PresignedPostPolicy">
#### PresignedPostPolicy(PostPolicy) (*url.URL, map[string]string, error)
Allows setting policy conditions to a presigned URL for POST operations. Policies such as bucket name to receive object uploads, key name prefixes, expiry policy may be set.

Create policy :

```go
policy := minio.NewPostPolicy()
```
Apply upload policy restrictions:

```go
policy.SetBucket("mybucket")
policy.SetKey("myobject")
policy.SetExpires(time.Now().UTC().AddDate(0, 0, 10)) // expires in 10 days

// Only allow 'png' images.
policy.SetContentType("image/png")

// Only allow content size in range 1KB to 1MB.
policy.SetContentLengthRange(1024, 1024*1024)

// Get the POST form key/value object:

url, formData, err := minioClient.PresignedPostPolicy(policy)
if err != nil {
    fmt.Println(err)
    return
}
```

POST your content from the command line using `curl`:

```go
fmt.Printf("curl ")
for k, v := range formData {
    fmt.Printf("-F %s=%s ", k, v)
}
fmt.Printf("-F file=@/etc/bash.bashrc ")
fmt.Printf("%s\n", url)
```

## 5. Bucket policy operations

<a name="SetBucketPolicy">
#### SetBucketPolicy(bucketname string, objectPrefix string, policy BucketPolicy) error
Set access permissions on bucket or an object prefix.

__Parameters__

<table>
    <thead>
        <tr>
            <th>Param</th>
            <th>Type</th>
            <th>Description</th>
        </tr>
    </thead>
    <tbody>
    <tr>
    <td>bucketName</td>
    <td>string</td>
    <td>name of the bucket</td>
    </tr>
    <tr>
    <td>objectPrefix</td>
    <td>string</td>
    <td>name of the object prefix</td>
    </tr>
    <tr>
            <td>
           policy
            </td>
            <td> BucketPolicy</td>
            <td>
            <ul>policy can be <br/> 
            <li>BucketPolicyNone,</li>
            <li>BucketPolicyReadOnly,</li>
            <li>BucketPolicyReadWrite,</li>
            <li>BucketPolicyWriteOnly</li>
            </ul>
            </td>
            </tr>
               </tbody>
</table>

__Return Values__

|Param   |Type   |Description   |
|:---|:---| :---|
|`err` | _error_  |standard error   |

__Example__

```go
err := minioClient.SetBucketPolicy("mybucket", "myprefix", BucketPolicyReadWrite)
if err != nil {
    fmt.Println(err)
    return
}
```

<a name="GetBucketPolicy">
#### GetBucketPolicy(bucketName string, objectPrefix string) (BucketPolicy, error)
Get access permissions on a bucket or a prefix.

__Parameters__

|Param   |Type   |Description   |
|:---|:---| :---|
|`bucketName`  | _string_  |name of the bucket.   |
|`objectPrefix` | _string_  |name of the object prefix   |

__Return Values__

|Param   |Type   |Description   |
|:---|:---| :---|
|`bucketPolicy`  | _BucketPolicy_ |string that contains: 'none', 'readonly', 'readwrite', or 'writeonly   |
|`err` | _error_  |standard error  |

__Example__

```go
bucketPolicy, err := minioClient.GetBucketPolicy("mybucket", "")
if err != nil {
    fmt.Println(err)
    return
}
fmt.Println("Access permissions for mybucket is", bucketPolicy)
```

## 6. Explore Further
- [Build your own Go Music Player App example](/docs/go-music-player-app)



















