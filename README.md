# Sync Files Across S3 Buckets and Accounts

If you need to sync files across AWS S3 buckets you should use `aws s3 sync`. But for cases where this won't work (e.g. multiple accounts without an assume role relationship) then this tool will sync via your local machine and multiple profiles.

This is useful for cases where you might want to copy files between environment stages or accounts which have granted you the operator permissions but not code running in them.

Essentially: S3 bucket source -> your machine -> S3 bucket target.

# TODO

- [X] Make it work for the simple case across two accounts
- [ ] Make it asynchronous
- [ ] Add parallelisation
- [ ] Implement S3 API requests directly, removing need for boto3
- [ ] Add task manager and rate limit adjustment to maximise throughput
