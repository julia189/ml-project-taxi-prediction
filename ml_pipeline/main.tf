module "crawler" {
    source = "./glue/crawler"

    crawlername = var.crawlername
    gluerole = var.gluerole
    region = var.region
    awsprofile = var.awsprofile
}

module "job" {
    source = "./glue/job"
    jobname = var.jobname
}

module "trigger" {
    source = "./glue/trigger"
}

module "sagemaker_config" {
    source = ".infrastructue/sagemaker_config"
}