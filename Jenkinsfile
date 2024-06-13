pipeline {
  agent any
  stages {
    stage('Checking out Codes') {
      parallel {
        stage('Checking out Codes') {
          steps {
            git(url: 'https://github.com/destor306/jenkins_test', branch: 'main')
          }
        }

        stage('Setup Python Environment') {
          steps {
            sh 'python3.8 -m venv intern'
            sh 'source intern/bin/activate'
            sh '''pip install psycopg2 geopandas pandas sqlalchemy=0.9.2 geoalchemy2=1.4.52
'''
          }
        }

      }
    }

    stage('run load csv files') {
      steps {
        dir(path: '/home/jason/Desktop/internship/scripts') {
          sh 'python loadscv_db.py'
        }

      }
    }

  }
}