pipeline {
  agent any
  stages {
    stage('Check Python Version') {
      steps {
        sh '''
which python3 || which python
                    python3 --version || python --version
'''
      }
    }

  }
}