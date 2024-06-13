pipeline {
  agent any
  stages {
    stage('Checking out Codes') {
      steps {
        git(url: 'https://github.com/destor306/jenkins_test', branch: 'main')
      }
    }

    stage('run load csv files') {
      steps {
        sh 'python /home/jason/Desktop/internship/scripts/loadcsv_db.py'
      }
    }

  }
}