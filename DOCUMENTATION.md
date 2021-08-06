# DeltaOMS Documentation Generation

DeltaOMS public documents are generated using [Hugo](https://gohugo.io) and 
published on [GitHub pages](https://gohugo.io/hosting-and-deployment/hosting-on-github/#deployment-of-project-pages-from-docs-folder-on-master-branch)

## Process to generate and publish the documentation

### Step-1 : Get the code

- Clone the DeltaOMS repository into your local directory using `git clone`
- Switch to the `dev` branch 

### Step-2 : Update the documents

- Navigate to the `docs` directory. The markdown contents used for generating the 
public document website are in the `content` folder
- Add/Modify the markdown files as required
- The Hugo generation is configured through the `config.toml` file
- Static resources like images and notebooks are stored inside the `static` folder
- Once your changes are made, you could test the site locally by using `hugo serve --buildDrafts`

###

