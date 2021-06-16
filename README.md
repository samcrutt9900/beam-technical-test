# beam-technical-test
## About
This is a project that satisfies a data science technical test.  
The test was split into two tasks. The branch feature-task1 has the code for the first task.  
The main branch is the refactored code as per task 2.

## Running
### Prerequisite
* Python version 3.8  
* pipenv 

### Ensure dependencies are installed locally 
``` pipenv install```

### Run
Run the following command at the root of the project.  
```pipenv run python src/beam.py```<br><br>
The 'GZ' file that is produced in the output folder can be unzipped using the `gunzip` command

### Tests
Run the following command to execute the unit test  
```pipenv run python -m unittest tests/test_beam.py```


