'''
From task to subtasks
For this exercise, you will be using parallel computing to apply the function take_mean_age() 
that calculates the average athlete's age in a given year in the Olympics events dataset. 
The DataFrame athlete_events has been loaded for you and contains amongst others, two columns:

Year: the year the Olympic event took place
Age: the age of the Olympian

You will be using the multiprocessor.Pool API
 which allows you to distribute your workload over several processes. 
 The function parallel_apply() is defined in the sample code. 
 It takes in as input the function being applied, the grouping used, 
 and the number of cores needed for the analysis. Note that the @print_timing decorator 
 is used to time each operation.
'''

# Function to apply a function over multiple cores
@print_timing
def parallel_apply(apply_func, groups, nb_cores):
    with Pool(nb_cores) as p:
        results = p.map(apply_func, groups)
    return pd.concat(results)

# Parallel apply using 1 core
parallel_apply(take_mean_age, athlete_events.groupby('Year'), 1)

# Parallel apply using 2 cores
parallel_apply(take_mean_age, athlete_events.groupby('Year'), 2)

# Parallel apply using 4 cores
parallel_apply(take_mean_age, athlete_events.groupby('Year'), 4)

'''
In the previous exercise, you saw how to split up a task and 
use the low-level python multiprocessing.Pool API to do calculations on several processing units.

It's essential to understand this on a lower level, but in reality, 
you'll never use this kind of APIs. A more convenient way to parallelize 
an apply over several groups is using the dask framework 
and its abstraction of the pandas DataFrame, for example.

The pandas DataFrame, athlete_events, is available in your workspace.
'''

import dask.dataframe as dd

# Set the number of pratitions
athlete_events_dask = dd.from_pandas(athlete_events, npartitions = 4)

# Calculate the mean Age per Year
print(athlete_events_dask.groupby('Year').Age.mean().compute())


'''
You've seen how to use the dask framework and its DataFrame abstraction to do some calculations. 
However, as you've seen in the video, in the big data world Spark is probably a more popular choice 
for data processing.

In this exercise, you'll use the PySpark package to handle a Spark DataFrame. 
The data is the same as in previous exercises: participants of Olympic events between 1896 and 2016.

The Spark Dataframe, athlete_events_spark is available in your workspace.

The methods you're going to use in this exercise are:

.printSchema(): helps print the schema of a Spark DataFrame.
.groupBy(): grouping statement for an aggregation.
.mean(): take the mean over each group.
.show(): show the results.
'''

# Print the type of athlete_events_spark
print(type(athlete_events_spark))

# Print the schema of athlete_events_spark
print(athlete_events_spark.printSchema())

# Group by the Year, and find the mean Age
print(athlete_events_spark.groupBy('Year').mean('Age'))

# Group by the Year, and find the mean Age
print(athlete_events_spark.groupBy('Year').mean('Age').show())

'''
Airflow DAGs
In Airflow, a pipeline is represented as a Directed Acyclic Graph or DAG. 
The nodes of the graph represent tasks that are executed. 
The directed connections between nodes represent dependencies between the tasks.

Representing a data pipeline as a DAG makes much sense, 
as some tasks need to finish before others can start. 
You could compare this to an assembly line in a car factory. 
The tasks build up, and each task can depend on previous tasks being finished.
A fictional DAG could look something like this:

Example DAG

Assembling the frame happens first, 
then the body and tires and finally you paint. 
Let's reproduce the example above in code.

First, the DAG needs to run on every hour at minute 0. 
Fill in the schedule_interval keyword argument using the crontab notation. 
For example, every hour at minute N would be N * * * *.
'''

# Create the DAG object
dag = DAG(dag_id="car_factory_simulation",
          default_args={"owner": "airflow","start_date": airflow.utils.dates.days_ago(2)},
          schedule_interval="0 * * * *")

# Task definitions
assemble_frame = BashOperator(task_id="assemble_frame", bash_command='echo "Assembling frame"', dag=dag)
place_tires = BashOperator(task_id="place_tires", bash_command='echo "Placing tires"', dag=dag)
assemble_body = BashOperator(task_id="assemble_body", bash_command='echo "Assembling body"', dag=dag)
apply_paint = BashOperator(task_id="apply_paint", bash_command='echo "Applying paint"', dag=dag)

# Complete the downstream flow
assemble_frame.set_downstream(place_tires)
assemble_frame.set_downstream(assemble_body)
assemble_body.set_downstream(apply_paint)