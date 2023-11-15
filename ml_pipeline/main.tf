module "crawler" {
    source = "./glue/crawler"

    crawlername = var.crawlername
    gluerole = var.gluerole
    region = var.region
    awsprofile = var.awsprofile
}
