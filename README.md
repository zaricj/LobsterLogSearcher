## Information about the Project
Log Searcher (specifically message logs) for the Lobster_data System.

Used to determine all input files that the System has processed in the last 7 days and writes the filesize for every file in a csv file, based on unique time value.

## Useful Command to create requirements.txt

You can use the following code to generate a requirements.txt file:

`pip install pipreqs`

`pipreqs /path/to/project`

The benefits of using pipreqs from its GitHub.

### Why not pip freeze?
`pip freeze only saves the packages that are installed with pip install in your environment.`

`pip freeze saves all packages in the environment including those that you don't use in your current project (if you don't have virtualenv).
and sometimes you just need to create requirements.txt for a new project without installing modules.`

Kuods to a the very useful [Stackoverflow answer](https://stackoverflow.com/questions/31684375/automatically-create-file-requirements-txt)

## Screenshot of Program
![alt text](/img/image.png)

