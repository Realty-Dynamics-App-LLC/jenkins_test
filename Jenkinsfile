pipeline {
  agent {
    node {
      label 'azure-vm'
    }

  }
  stages {
    stage('checking out code') {
      steps {
        git(url: 'https://github.com/Realty-Dynamics-App-LLC/jenkins_test', branch: 'main')
      }
    }

  }
}