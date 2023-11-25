resource "aws_glue_trigger" "test-trigger" {
    name = "test-trigger"
    schedule = "cron(25 5 * * ?)"
    type = "SCHEDULED"
    actions {
        job_name = aws_glue_job.preprocessing-etl.name
    }
}