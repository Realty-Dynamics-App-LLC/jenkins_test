pipeline {
  agent any
  stages {
    stage('Setup Python Environment') {
      steps {
        sh 'sudo apt install python3.12-venv'
        sh '''
python3 -m venv venv
                    . venv/bin/activate
                    pip install -r requirements.txt
'''
      }
    }

  }
}