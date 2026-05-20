output "raw_bucket_name" {
  value = aws_s3_bucket.raw.id
}

output "curated_bucket_name" {
  value = aws_s3_bucket.curated.id
}

output "models_bucket_name" {
  value = aws_s3_bucket.models.id
}

output "outputs_bucket_name" {
  value = aws_s3_bucket.outputs.id
}
