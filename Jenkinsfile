pipeline {
  agent any
  stages {
    stage('Setup Environment') {
      steps {
        script {
          withCredentials([file(credentialsId: 'internship', variable: 'PEM_FILE_PATH')]) {
            // Verify the .pem file content or existence
            sh 'ls -l $PEM_FILE_PATH'  // List the file to verify it exists
            sh 'cat $PEM_FILE_PATH'    // Optionally display the file content
          }
        }

      }
    }

  }
}