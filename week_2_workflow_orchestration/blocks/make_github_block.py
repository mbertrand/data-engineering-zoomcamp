from prefect.filesystems import GitHub

block = GitHub(
    repository="https://github.com/mbertrand/data-engineering-zoomcamp.git"
)
block.save("zoom")