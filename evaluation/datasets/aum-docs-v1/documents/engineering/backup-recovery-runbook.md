# Object-storage recovery runbook

For accidental deletion from object storage, first stop lifecycle jobs and
identify the affected bucket and version IDs. Restore the required object
versions from the retention window, then verify checksums and application
access before reopening writes.

Escalate if the retention window has expired. Backups are for recovery; they
do not change the legal retention schedule for customer records.
