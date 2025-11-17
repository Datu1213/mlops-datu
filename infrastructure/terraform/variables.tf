# variables.tf

variable "aws_region" {
  description = "The AWS region to deploy resources in."
  type        = string
  default     = "us-west-2"
}

variable "project_name" {
  description = "The name of the project, used to prefix resource names."
  type        = string
  default     = "ml-infra-prod"
}