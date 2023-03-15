

##### Terraform
- Set up a project in Google Cloud, create a service account for it, adn generate/download keys in JSON format
  
- Create an .env file with the following:
   ```
   export TF_VAR_project=mattsat
   export GOOGLE_APPLICATION_CREDENTIALS=<path/to/your service credentials json file>
   ```
- Authenticate with google cloud via the CLI
  ```
  gcloud auth application-default login
  ```  

- Go to the terraform folder and run:
  ```
  terraform apply
  ```


#### Prefect

###### Initial setup
- Run each of the python files in the blocks folder
