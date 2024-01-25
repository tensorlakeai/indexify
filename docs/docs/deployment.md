# Deployment 

This resource is designed to assist developers in deploying and managing an efficient, scalable, and secure EKS (Elastic Kubernetes Service) infrastructure on Amazon Web Services (AWS) using Terraform and Kubernetes. We have [open sourced an example here](https://github.com/tensorlakeai/indexify-aws-deployment) including a comprehensive README tailored to streamline your journey from setup to production. It outlines a step-by-step process divided into eight main sections:

#####1. Install AWS + Terraform CLI 
Instructions on installing the Terraform CLI, AWS CLI, and eksctl.

#####2. Create AWS Credentials 
Guidance on creating a new user in the AWS IAM console, generating access keys, and configuring the AWS CLI.

#####3. Setup IAM Policy + Role 
Details on creating a new policy named TerraformPolicy and assigning it to the user.

#####4. Initialize Terraform and Apply Resources 
Steps to initialize Terraform in the appropriate directory, modify necessary variables, and apply the configuration.

#####5. Configure Kubernetes Files 
Instructions on setting up kubeconfig, updating Kubernetes configuration files, and setting AWS access keys in environment variables.

#####6. k8 Load Balancer Setup 
A comprehensive walkthrough for setting up a Kubernetes load balancer, including creating an IAM policy, a service account, an OIDC provider, and installing the AWS Load Balancer.

#####7. Apply the Rest of the k8 Configuration 
Guidance on applying the remaining Kubernetes configurations.

#####8. Tests 
Instructions on how to test the setup, including adding data, binding a miniLM extractor, getting indexes, and performing a search.