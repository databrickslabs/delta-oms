# DeltaOMS Documentation Generation

DeltaOMS uses [Hugo](https://gohugo.io) to generate public documentation. These generated site 
is published [here](https://databrickslabs.github.io/delta-oms/) using [GitHub pages](https://gohugo.io/hosting-and-deployment/hosting-on-github/#deployment-of-project-pages-from-docs-folder-on-master-branch)

## Process to generate and publish the documentation

### Automated

DeltaOMS has a built-in github action ,under [gh-pages.yml](./.github/workflows/gh-pages.yml),
to publish the docs automatically when changes are made to the documents on the `dev` branch.
This is the preferred approach for updating documents. 

The manual steps for generating documents is listed below.

### Manual

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

### Step-3  : Update the documentation branch

- Delete the `public` folder under `docs` (if exists)
- Checkout the `public_docs` branch as a worktree `git worktree add -B public_docs public origin/public_docs`
- Generate the site using `hugo`. This will create the static site under `public` directory
- Change to the `public` directory and push changes to the `public_docs` branch 
- This will trigger the publish process and update the documentation

