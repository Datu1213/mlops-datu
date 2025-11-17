# outputs.tf

output "github_actions_deployer_role_arn" {
  description = "The ARN of the IAM role for GitHub Actions to assume."
  value       = aws_iam_role.github_actions_deployer.arn
}