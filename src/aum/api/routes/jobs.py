from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from aum.api.deps import get_tracker, require_admin
from aum.auth.models import User

router = APIRouter(prefix="/api/jobs", tags=["jobs"])


class JobErrorResponse(BaseModel):
    file_path: str
    error_type: str
    message: str
    timestamp: str


class JobResponse(BaseModel):
    job_id: str
    job_type: str
    source_dir: str
    status: str
    total_files: int
    processed: int
    failed: int
    created_at: str
    finished_at: str | None


class JobDetailResponse(JobResponse):
    errors: list[JobErrorResponse]


@router.get("", response_model=list[JobResponse])
async def list_jobs(
    user: Annotated[User, Depends(require_admin)],
    status: str | None = None,
) -> list[JobResponse]:
    tracker = get_tracker()
    from aum.models import JobStatus

    job_status = JobStatus(status) if status else None
    jobs = tracker.list_jobs(status=job_status)
    return [
        JobResponse(
            job_id=j.job_id,
            job_type=j.job_type.value,
            source_dir=str(j.source_dir),
            status=j.status.value,
            total_files=j.total_files,
            processed=j.processed,
            failed=j.failed,
            created_at=j.created_at.isoformat(),
            finished_at=j.finished_at.isoformat() if j.finished_at else None,
        )
        for j in jobs
    ]


@router.get("/{job_id}", response_model=JobDetailResponse)
async def get_job(
    job_id: str,
    user: Annotated[User, Depends(require_admin)],
) -> JobDetailResponse:
    tracker = get_tracker()
    job = tracker.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    return JobDetailResponse(
        job_id=job.job_id,
        job_type=job.job_type.value,
        source_dir=str(job.source_dir),
        status=job.status.value,
        total_files=job.total_files,
        processed=job.processed,
        failed=job.failed,
        created_at=job.created_at.isoformat(),
        finished_at=job.finished_at.isoformat() if job.finished_at else None,
        errors=[
            JobErrorResponse(
                file_path=str(e.file_path),
                error_type=e.error_type,
                message=e.message,
                timestamp=e.timestamp.isoformat(),
            )
            for e in job.errors
        ],
    )
