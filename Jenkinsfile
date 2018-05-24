#!groovy
// -*- mode: groovy -*-

def finalHook = {
  runStage('store CT logs') {
    archive '_build/test/logs/'
  }
}

build('mg-api-erlang', 'docker-host', finalHook) {
  checkoutRepo()
  loadBuildUtils()

  def pipeDefault
  def withWsCache
  runStage('load pipeline') {
    env.JENKINS_LIB = "build-utils/jenkins_lib"
    pipeDefault = load("${env.JENKINS_LIB}/pipeDefault.groovy")
    withWsCache = load("${env.JENKINS_LIB}/withWsCache.groovy")
  }

  def masterlike() {
    return (env.BRANCH_NAME == 'master' || env.BRANCH_NAME.startsWith('epic'))
  }

  pipeDefault() {

    if (!masterlike()) {

      runStage('compile') {
        withGithubPrivkey {
          sh 'make wc_compile'
        }
      }

      runStage('lint') {
        sh 'make wc_lint'
      }

      runStage('xref') {
        sh 'make wc_xref'
      }

      runStage('dialyze') {
        withWsCache("_build/default/rebar3_19.1_plt") {
          sh 'make wc_dialyze'
        }
      }

      runStage('test') {
        sh "make wdeps_test"
      }

    }

  }
}
