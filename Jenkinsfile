pipeline {
  agent {
    node {
      label 'azure-vm'
    }

  }
  stages {
    stage('Checkout Code') {
      steps {
        git(url: 'https://github.com/destor306/jenkins_test', branch: 'main')
      }
    }

    stage('Log') {
      steps {
        sh 'ls -la'
      }
    }

    stage('Docker build image') {
      steps {
        sh 'docker build -t pipeline .'
      }
    }

    stage('log into Dockerhub') {
      steps {
        sh 'docker login -u destor306'
      }
    }

  }
}