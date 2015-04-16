# Jake's Special Hacks on the River

## URL Encoding

One issue with the river that we discovered pretty early is that Amazon S3 allows you to create documents with names that cause the Java XML parser to throw exceptions when listing them for indexing. The problem is that the Amazon API for some reason returns the S3 XML as with a `<xml version="1.0">` header. This does not allow certain characters in the document (unlike version 1.1). The hackish solution is to create the initial request to S3's `listObjects()` with an additional `withEncoding("url")` argument that URL-encodes the names of all document keys. This meant also adding a method to decode the keys returned in the listObjects XML so that retrieval and querying metadata about the document would work and not return a 404 Not Found

## Document IDs

The S3 river created IDs for documents from the river by using the S3 path. This meant that IDs were extremely long and contained unexpected characters like spaces and slashes. In my postprocessing script, I decided it would be useful to save attachments from emails inside a folder named with the document ID. This old ID scheme was horrendous for that, so instead I compute a SHA-256 hash of the S3 path and use a modified version of the Base64 string as the document ID. This means that IDs are more "randomly" distributed and shorter. For details of that, see the `S3River.buildIndexIdFromS3Key()` method

## Memory Footprint

Like most ElasticSearch rivers, the S3 river falls apart at scale. We were experiencing problems when trying to index an S3 bucket with more than a million documents. Even when ElasticSearch's heap was bumped up to the maximum of 2gb, it would run out of heap space trying to run the river. This showed up as an Out-of-Memory exception when parsing an XML document, but the real problem was the storage used to track new & changed documents in the river. To explain, this is how the S3 river works:

1. In a loop, run through all keys found in a bucket using the `listObjects` and `listNextBatchOfObjects` methods to paginate through.
2. For each document, check to see if the file modification date is greater than the last time the river was run. If the river is new, this time is overridden to 0 so all documents are considered changed.
3. If a document is changed, add the S3 metadata about it to a vector of changed documents called `result`
4. Regardless of whether the document is changed or not, record its key to another vector `keys` of all keys found in the bucket. This is used for tracking deletions.

This code is in the `S3Connector.getObjectSummaries` method. Once it finishes, the River then does two things with the returned data in the `S3River.scan` method:

1. It runs through the `result` vector and adds any documents that are indexable (ie, not excluded by regexp) to be pulled down in batches of 100 from S3 into ElasticSearch
2. It runs through the `keys` vectors to see if there are documents in the ES index that are not on S3. If so, it deletes them to match what is in S3. It does this by building up another list of all keys in the ES index and iterating through to see if they are in the S3 `keys` vector. This is done with two List objects (not Sets, so it's not particularly efficient anyway. Yikes)

To tackle the memory issues, I made several different fixes. First, since we are just trying to index large, static buckets on S3, we probably don't have many cases where a document will be deleted on S3 and need to be deleted in the index. So, that scanning is wasteful in many ways. To fix this, I added an optional flag `deleteS3` that can be passed in as part of the river creation JSON. It defaults to true, but if set to false it both doesn't add documents to the `keys` vector and skips the check to see if any documents were deleted on S3. This saves a large chunk of memory that would be used on each run, even if nothing had changed like would be the case for many of our static datasets.

Still, there is an issue that the river has to build up a large `result` vector on its first run. To keep it from consuming all memory for large indices, I modified the river to only index up to 100,000 new documents on a given run. For this to work, I had to make a few changes to how the river works:

1. I added a boolean to indicate whether or not the river was in `initial_scan` mode. If it is not explicitly set, it defaults to true. If in `initial_scan` mode, it sleeps for a shorter interval than it would normally before the next scan.
2. The river also keeps track of the last key it scanned as a bookmark to start the next scan run from. If the river encounters 100,000 new or changed documents on a run, it stops any further scanning, sets `initial_scan` to true if it wasn't already and stores the last key it scanned to start from the next time.
3. If the scan finds less than 100,000 new documents, it sets `initial_scan` to false and clears the scan bookmark. All future scans will just look for documents newer than the last scan time.

This constant can be adjusted upwards or downwards – there is nothing magical about 100,000 – but it probably makes sense to make it a multiple of 100, since that is what the river then batches together.