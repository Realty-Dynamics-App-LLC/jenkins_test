pipeline {
  agent any
  stages {
    stage('Setup Python Environment') {
      steps {
        sh '''sh \'python3 -m venv venv\'
sh \'. venv/bin/activate\'
sh \'pip install -r requirements.txt\''''
      }
    }

  }
  environment {
    DB_HOST = '172.208.27.131'
    DB_PORT = '5432'
    DB_USER = 'postgres'
    DB_PASSWORD = '1234'
  }
}