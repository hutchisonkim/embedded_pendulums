

    Change Log
    ===========

    2025-05-16: Initial release of the repository.

## ⚛️ Embedded Pendulums


Embedded Pendulums is a data pipeline to experiment with the relationship between rotational motion and the curvature of its resulting translational path. Key aspects of the approach include:

1. **Distributed Processing**: Spark SQL commands are used to process rotation and translation data across multiple nodes for high performance.
2. **Serialization**: Intermediate data is serialized into CSV files, ensuring compatibility and portability between tasks.
3. **B-Spline Representation**: Curvature data is processed to derive a basis spline model, which can be used for further analysis and visualization.


## 📂 Repository Contents

- **`/src`**: Core implementation of the pipeline, including io, test, validation and transformation logic.
- **`/data`**: Example input and output datasets.
- **`/dags`**: Entry point for running the development graph.


## ⚙️ Getting Started

### Prerequisites

- Git
- Docker
- Visual Studio Code (optional, but recommended for development)

### Installation

1. Clone the repository:
   ~~~bash
   git clone https://github.com/hutchisonkim/effector_transformations.git
   ~~~

2. Build and start the container:
   ~~~bash
   docker-compose up --build
   ~~~

## 💻 Running the Code
### Pipeline Evaluation

1. Start a run on the pipeline:
   - Browse to `http://localhost:8080/dags/embedded_pendulums_development_dag/`
   - Trigger the DAG by clicking on the "Trigger DAG" button (the play icon) in the top right corner

2. View the results:
   - The output PNG and CSV files will be available in the `/data/` folder
   - Log files for each task and run can be found in the `/logs/` folder



##  ✒️ Dev Features

- ### [Hot Reload] Sensor DAG Tasks

    Sensor tasks are used to monitor changes in the code files. When a change is detected, the DAG is reloaded, and the pipeline is re-evaluated. This allows for rapid iteration and testing of new features without needing to restart the entire pipeline.

- ### [Hot Reload] Forever Notebook

    Bypassing the DAG, you can use the development notebook at `/notebooks/embedded_pendulums_notebook.ipynb` to run tasks and monitor code files as you make changes to them.
    
- ### [QA] Unit Test Tasks

    Between each sensor and generator task pair is a test task to catch any failure introduced by the code file changes detected by the sensor task.

- ### [QA] Data Validation Tasks

    After each generator task is a validator task that checks the generated data against its schema.


