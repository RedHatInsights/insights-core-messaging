# Contributing to insights-core-messaging

## Introduction

The insights-core-messaging project provides a framework for the
building of insights archive processing applications.

The framework is implemented in Python 3.7+.  The code management uses
the [gitflow](https://nvie.com/posts/a-successful-git-branching-model/)
branching strategy.  So normal, feature contributions should generally
be created on topic branches branching from the _develop_ branch.
Submissions would then be made as Pull Requests (PRs) back to the
_develop_ branch.

## Contributor Setup

If you wish to contribute to the insights-core-messaging project you'll
need to create a fork in github.  Then

1. Clone your fork::

    git clone git@github.com:your-user/insights-core-messaging.git
    cd insights-core-messaging

2. Reference the original project as "upstream"::

    git remote add upstream git@github.com:RedHatInsights/insights-core-messaging.git

3. Synchronize your fork with the upstream project using the following
   commands::

    git pull upstream master
    git push origin master

4. To setup a virtual environment and install dependencies

    pytho3 -m venv .
    source bin/activate
    pip install --upgrade pip
    pip install -e .[develop]

## Contributor Submissions

Contributors should submit changes to the code via github "Pull
Requests."  One would normally start a new contribution with a branch
from the current _develop_ branch of the upstream project.

1. Synchronize your fork of the _develop_ branch:

    git pull upstream develop
    git push origin develop

2. Make a branch on the fork.  Use a branch name that would be
   meaningful as it will be part of a default commit message when the
   topic branch is merged into the upstream project
   
    git checkout -b your-topic
   
3. Make contributions on the topic branch.  This project uses the
   [DCO](https://developercertificate.org/) to manage contributions. Commits
   must be signed by you in order to be accepted. To sign a commit simply add
   `-s` to the commit command.

    git commit -s

   Push commits to your fork (creating a remote topic branch on your fork)

    git push

4. If you need to make updates after pushing, it is useful to rebase
   with develop.  This will change history, so you will need to force the
   push (this is fine on a topic branch when other developers are not
   working from the remote branch.)

    git checkout develop
    git pull --rebase upstream develop
    git push
    git checkout your-topic
    git rebase develop
    git push --force

5. Generally, keep the number of commits on the topic branch small.
   Usually a single commit, perhaps a few in some cases.  Use the
   `amend` and `rebase -i` git commands to manage the commit history
   of the topic branch.  Again, such manipulations change history and
   require a `--force` push.

6. When ready, use the github UI to submit a pull request.

7. Repeat steps 4 and 5 as necessary.  Note that a forced push to the
   topic branch will work as expected.  The pull request will be
   updated with the current view of the topic-branch.

## Style Conventions

### Code Style

Code style mostly follows [PEP8](https://www.python.org/dev/peps/pep-0008/).
The style followed is essentially encoded in the
[flake8](http://flake8.pycqa.org/en/latest/) configuration file in the
repo's root directory.  The current configuration specifies the
following rules as exceptions

- E501: Line too long
- E126: Continuation line over-indented for hanging indent
- E127: Continuation line over-indented for visual indent
- E128: Continuation line under-indented for visual indent
- E722: Bare Except
- E741: Do not use variables named 'l', 'o', or 'i'

In some cases, a particular bit of code may require formatting that
violates flake8 rules.  In such cases, one can, for example, annotate
the line with ``# noqa``.  Override flake8 checking sparingly.

Code that does not pass the project's current flake8 tests
will not be accepted.

### Commit Message Style

Commit messages are an important description of changes taking place in
the code base. So, they should be effective at providing useful
descriptions of the changes for someone browsing the git log.

Generally, they should follow the usual
[git conventions](http://chris.beams.io/posts/git-commit/).

1. Separate subject from body with a blank line
2. Limit the subject line to 50 characters
3. Capitalize the subject line
4. Do not end the subject line with a period
5. Use the imperative mood in the subject line
6. Wrap the body at 72 characters
7. Use the body to explain the *what* and *why* vs. *how*


### Documentation

Code should generally be clear enough to self-document the *how* of the
implementation.  Of course, when a bit of code isn't clear, comments may
be needed.

Documentation in the form of pydoc should be considered to document
usage of code as necessary.

## Review Checklist

The following checklist is used when reviewing pull requests

### General (all submissions)

- Commit messages are useful and properly formatted
- Unit tests validate the code submission
- One commit, or at most only a handful.  More than five commits should
  be heavily questioned
