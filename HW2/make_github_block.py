from prefect.filesystems import GitHub

block = GitHub(
    repository="https://github.com/svetlanakononova/data-engineering-zoomcamp_answers.git",
)
block.save("github-deploy", True)