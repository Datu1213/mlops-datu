# main.tf

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # backend "s3" {
  #   bucket  = "ml-terraform-state-bucket"
  #   key     = "infrastructure/terraform.tfstate"
  #   region  = "us-west-2"
  #   encrypt = true
  # }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "ML Infrastructure"
      Environment = var.project_name
      ManagedBy   = "Terraform"
    }
  }
}

# For eks version < 20, uncomment below to enable kubernetes provider
#
# provider "kubernetes" {
#   host                   = module.eks.cluster_endpoint
#   cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
#   token                  = data.aws_eks_cluster_auth.this.token
# }

# data "aws_eks_cluster" "this" {
#   name = module.eks.cluster_name
# }

# data "aws_eks_cluster_auth" "this" {
#   name = module.eks.cluster_name
# }

# Use module to create VPC
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "${var.project_name}-vpc"
  cidr = "10.0.0.0/16"

  azs             = ["${var.aws_region}a", "${var.aws_region}b"]
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24"]

  enable_nat_gateway   = true
  enable_dns_hostnames = true
}

# 使用VPC的输出，创建一个EKS K8s集群
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = "${var.project_name}-cluster"
  cluster_version = "1.28"

  # 建立私有连接和公有连接端点
  cluster_endpoint_public_access  = true
  cluster_endpoint_private_access = true

  # 仅允许特定的IP范围访问公有端点
  # cluster_endpoint_public_access_cidrs = ["203.0.113.5/32", "YOUR_OFFICE_IP/32"] 

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  eks_managed_node_groups = {
    general = {
      name           = "general-nodes"
      instance_types = ["m5.large"]
      min_size       = 2
      max_size       = 5
      desired_size   = 3
    }
  }

  # For eks version < 20, uncomment below to enable aws_auth configmap management
  #
  # manage_aws_auth_configmap = true   # 确保 Terraform 管理 aws-auth

  # aws_auth_roles = [{
  #   rolearn  = aws_iam_role.github_actions_deployer.arn
  #   username = "github-actions-deployer"
  #   groups   = ["system:masters"] # 授予完整的集群管理员权限
  # }]
}


# 为GitHub Actions创建一个OIDC身份提供商
resource "aws_iam_openid_connect_provider" "github" {
  url             = "https://token.actions.githubusercontent.com"
  client_id_list  = ["sts.amazonaws.com"]
  # GitHub OIDC的固定指纹
  thumbprint_list = ["6938fd4d98bab03faadb97b34396831e3780aea1"]
}

# 创建一个专门给GitHub Actions扮演的角色
resource "aws_iam_role" "github_actions_deployer" {
  name = "github-actions-eks-deployer-role"

  # 信任策略：只允许来自指定GitHub仓库的请求扮演此角色
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          # GitHub OIDC身份提供商
          # template: arn:aws:iam::<AWS_ACCOUNT_ID>:oidc-provider/token.actions.githubusercontent.com
          # lauch "aws sts get-caller-identity --query "Account" --output text" to get AWS_ACCOUNT_ID
          Federated = "arn:aws:iam::039992076397:oidc-provider/token.actions.githubusercontent.com"
        },
        Action = "sts:AssumeRoleWithWebIdentity",
        Condition = {
          StringLike = {
            # 限制只能是特定仓库的请求
            # template: "repo:<GITHUB_OWNER>/<GITHUB_REPO>:*"
            "token.actions.githubusercontent.com:sub" = "repo:Datu1213/ml-infrastructure-bootcamp:*"
          }
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "github_actions_deployer_eks_attach" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
  role       = aws_iam_role.github_actions_deployer.name
}

resource "aws_iam_role_policy_attachment" "github_actions_deployer_eks_worker_attach" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.github_actions_deployer.name
}

# IAM Identity Mapping (for EKS version >= 20)
# Use Access Entry and Access Policy Association

resource "aws_eks_access_entry" "github_actions" {
  cluster_name  = module.eks.cluster_name
  principal_arn = aws_iam_role.github_actions_deployer.arn
  type          = "STANDARD"
}

resource "aws_eks_access_policy_association" "github_actions_admin" {
  cluster_name  = module.eks.cluster_name
  policy_arn    = "arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"
  principal_arn = aws_iam_role.github_actions_deployer.arn
  access_scope {
    type = "cluster"
  }
}
