#!groovy
import hudson.tasks.test.AbstractTestResultAction
import net.sf.json.JSONArray
import net.sf.json.JSONObject

String branchName = env.BRANCH_NAME ? env.BRANCH_NAME : scm.branches.first().getExpandedName(env.getEnvironment())

def convertProjectName(String projectName) {
    return projectName.replace(" ", "_").replace("/", "_").toLowerCase()
}

def PROJECT_NAME = env.BRANCH_NAME ? convertProjectName(env.JOB_NAME + "_" + env.BRANCH_NAME) : convertProjectName(env.JOB_NAME)

boolean isAfterTestsToBeRun = false

pipeline {
    agent any
    stages {
        stage('Init') {
            options {
                timeout(time: 15, unit: 'MINUTES')
            }
            steps {
                script {
                    printEnvVariables()
                    script {
                        echo "'branchName' => '" + branchName + "'"
                        params.each { key, value ->
                            echo "'" + String.valueOf(key) + "' => '" + String.valueOf(value) + "'"
                        }
                        notifySlack('STARTED', 'Linux', null, branchName)
                    }
                }
            }
        }
        stage('Ignite Migration Tests') {
            options {
                timeout(time: 15, unit: 'MINUTES')
            }
            steps {
                script {
                    isAfterTestsToBeRun = true
                    sh "#!/bin/bash -xe\n\
                       cd ./products\n\
                       docker-compose --project-name ${PROJECT_NAME} --compatibility -f docker-compose.yml down -v -t 60 --rmi all --remove-orphans || true\n\
                       docker-compose --project-name ${PROJECT_NAME} --compatibility -f docker-compose.yml build --no-cache\n\
                       docker-compose --project-name ${PROJECT_NAME} --compatibility -f docker-compose.yml up \
                           --force-recreate --abort-on-container-exit --exit-code-from tests_ignitemigrationtool --no-color\n\
                       docker-compose --project-name ${PROJECT_NAME} --compatibility -f docker-compose.yml down -v -t 60 --rmi all --remove-orphans\n\
                       cd .."
                }
                junit 'products/core/tests-ignitemigrationtool/surefire-reports/*.xml'
                script {
                     isAfterTestsToBeRun = false
                }
            }
        }
    }

    post {
        always {
            script {
                try {
                    if (isAfterTestsToBeRun) {
                        sh "#!/bin/bash -xe\n\
                            cd ./products\n\
                            docker-compose --project-name ${PROJECT_NAME} --compatibility -f docker-compose.yml down -v -t 60 --rmi all --remove-orphans || true\n\
                            cd .."
                    }
                }
                catch (Throwable e) {
                    e.printStackTrace()
                }
                try {
                    if (isAfterTestsToBeRun) {
                        junit 'products/core/tests-ignitemigrationtool/surefire-reports/*.xml'
                    }
                }
                catch (Throwable e) {
                    e.printStackTrace()
                }

                checkResultAndSetBuildStatus()

                notifySlack((String) currentBuild.currentResult, 'Linux', null, branchName, null)
            }
        }
    }
}

@NonCPS
def checkResultAndSetBuildStatus() {
    def build = currentBuild.rawBuild.getAction(AbstractTestResultAction.class)
    //typically SUCCESS, UNSTABLE, or FAILURE. Will never be null.
    if (build != null && build.getSkipCount() > 0) {
        currentBuild.result = "UNSTABLE"
    }
}

@NonCPS
def printEnvVariables() {
    println "'NODE_NAME' = '${env.NODE_NAME}'"
    env.getEnvironment().each { name, value -> println "'$name' = '$value'" }
}

def getLastCommitId(String currentBranchName) {
    LAST_COMMIT_ID = sh(
            script: "#!/bin/bash -e\ngit rev-parse origin/${currentBranchName}",
            returnStdout: true
    ).trim()
    return LAST_COMMIT_ID
}

def notifySlack(String buildStatus, String system, String channel = null, String currentBranchName, String customMessage = null) {

    buildStatus = buildStatus ?: 'SUCCESS'
    // Default values
    def subject = "${buildStatus}: Job '" + URLDecoder.decode("${env.JOB_NAME}", "UTF-8") + " [${env.BUILD_NUMBER}] on ${system} (<${env.RUN_DISPLAY_URL}|Open>) (<${env.RUN_CHANGES_DISPLAY_URL}|  Changes>)'"
    def title = URLDecoder.decode("${env.JOB_NAME}", "UTF-8") + " Build: ${env.BUILD_NUMBER}"
    def title_link = "${env.RUN_DISPLAY_URL}"
    def duration = currentBuild.getDurationString().replace(' and counting', '')

    def colorCode
    def author
    def message

    author = sh(returnStdout: true, script: '#!/bin/bash -e\ngit --no-pager show -s --pretty=format:\"\n\"Hash\":\"%h\",\n\"Author\":\"%cn\",\n\"CommitDate\":\"%cd\"\n\"').trim()
    message = sh(returnStdout: true, script: '#!/bin/bash -e\ngit --no-pager log -1 --pretty=format:\"%B\"').trim()

    // Override default values based on build status
    if (buildStatus == 'STARTED') {
        color = 'YELLOW'
        colorCode = '#FFFF00'
    }
    else if (buildStatus == 'SUCCESS') {
        color = 'GREEN'
        colorCode = 'good'
    }
    else if (buildStatus == 'UNSTABLE') {
        color = 'YELLOW'
        colorCode = 'warning'
    }
    else {
        color = 'RED'
        colorCode = 'danger'
    }

    // get test results for slack message
    @NonCPS
    def getTestSummary = { ->
        def testResultAction = currentBuild.rawBuild.getAction(AbstractTestResultAction.class)
        def summary = ""

        if (testResultAction != null) {
            def total = testResultAction.getTotalCount()
            def failedCount = testResultAction.getFailCount()
            def failedNames = testResultAction.getFailedTests()
            def skipped = testResultAction.getSkipCount()
            def failures = [:]

            summary = 'Test results:\n\t'
            summary += 'Passed: ' + (total - failedCount - skipped)
            summary += ', Failed: ' + failedCount
            summary += ', Skipped: ' + skipped + '\n'
            summary += "Failed tests:\n"
            failedNames.each { test ->
                summary += "\n-------------------------------------------------\n"
                failures.put(test.fullDisplayName, test)
                summary += "Name: ${test.name}\n"

                if (test.errorDetails != null) {
                    def errorMessage = test.errorDetails.split("\n")[0]
                    summary += "Error message: ${errorMessage}\n"
                }
                else {
                    summary += "Error message: unavailable\n"
                }

                if (test.errorStackTrace != null) {
                    def stackTraceStrings = Arrays.asList(test.errorStackTrace.split("\n")).find {
                        it.trim().startsWith("at alliedplugins")
                    }
                    if (stackTraceStrings != null) {
                        def stackTrace = stackTraceStrings.join(",\n")
                        summary += "Possible reason: ${stackTrace}\n"
                    }
                }
                else {
                    summary += "Possible reason: unknown\n"
                }
            }
        }
        else {
            summary = "No tests found"
        }
        return summary
    }
    def testSummaryRaw = getTestSummary()
    // format test summary as a code block
    String testSummary = "${testSummaryRaw}"

    JSONObject attachment = new JSONObject()
    attachment.put('author', "jenkins")
    attachment.put('title', title.toString())
    attachment.put('title_link', title_link.toString())
    attachment.put('text', subject.toString())
    attachment.put('fallback', "fallback message")
    attachment.put('color', colorCode)
    attachment.put('mrkdwn_in', ["fields"])

    JSONObject branch = new JSONObject()
    branch.put('title', 'Branch')
    branch.put('value', "<https://alliedplugins.atlassian.net/browse/" + currentBranchName + "|" + currentBranchName + ">")
    branch.put('short', true)

    JSONObject commitAuthor = new JSONObject()
    commitAuthor.put('title', 'Commit info')
    commitAuthor.put('value', author)
    commitAuthor.put('short', false)

    JSONObject commitMessage = new JSONObject()
    commitMessage.put('title', 'Last Commit Message')
    commitMessage.put('value', message + "\n" + "<https://bitbucket.org/allied_testing/ignitemigrationtool/commits/" + getLastCommitId(currentBranchName) + "|Last commit>")
    commitMessage.put('short', false)

    JSONObject durationInfo = new JSONObject()
    durationInfo.put('title', 'Build duration')
    durationInfo.put('value', duration)
    durationInfo.put('short', true)

    JSONObject failureReason = new JSONObject()
    if (customMessage != null) {
        durationInfo.put('title', 'Failure reason')
        durationInfo.put('value', customMessage)
        durationInfo.put('short', true)
    }

    JSONObject testResults = new JSONObject()
    testResults.put('title', 'Test Summary')
    testResults.put('value', testSummary)
    testResults.put('short', false)

    if (buildStatus == 'STARTED') {
        attachment.put('fields', [branch, commitAuthor, commitMessage])
    }
    else {
        attachment.put('fields', [branch, commitAuthor, commitMessage, customMessage != null ? failureReason : null, durationInfo, testResults])
    }

    JSONArray attachments = new JSONArray()
    attachments.add(attachment)

    // Send notifications
    if (channel != null) {
        slackSend(color: colorCode, message: message, attachments: attachments.toString(), channel: channel)
    }
    else {
        slackSend(color: colorCode, message: message, attachments: attachments.toString())
    }
}