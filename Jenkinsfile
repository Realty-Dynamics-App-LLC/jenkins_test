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
        dir(path: '/home/jason/Desktop/internship/scripts') {
          sh 'python loadscv_db.py'
        }

      }
    }

  }
}