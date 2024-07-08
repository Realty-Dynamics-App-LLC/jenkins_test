pipeline {
  agent any
  stages {
    stage('set environment') {
      steps {
        sh '''withCredentials([file(credentialsID: \'internship\', variable: \'PEM_FILE\')]){
sh \'cp $PEM_FILE $PEM_FILE_PATH\'
sh \'chmod 400 $PEM_FILE_PATH\'
}'''
        }
      }

    }
  }