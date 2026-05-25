#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import sys
from github import Github
import os
import re
import subprocess

def print_pulls(repo_name, title, pulls):
    if len(pulls)  > 0:
        print("**{}:**".format(title))
        print()
        for (pull, commit) in pulls:
            url = "https://github.com/{}/pull/{}".format(repo_name, pull.number)
            print("- {} [#{}]({}) ({})".format(pull.title, pull.number, url, commit.author.login))
        print()


def generate_changelog(repo, repo_name, tag1, tag2, version):

    # get a list of commits between two tags
    print(f"Fetching list of commits between {tag1} and {tag2}", file=sys.stderr)
    comparison = repo.compare(tag1, tag2)

    # get the pull requests for these commits
    print("Fetching pull requests", file=sys.stderr)
    unique_pulls = []
    all_pulls = []
    for commit in comparison.commits:
        pulls = commit.get_pulls()
        for pull in pulls:
            # there can be multiple commits per PR if squash merge is not being used and
            # in this case we should get all the author names, but for now just pick one
            if pull.number not in unique_pulls:
                unique_pulls.append(pull.number)
                all_pulls.append((pull, commit))

    # we split the pulls into categories
    breaking = []
    bugs = []
    docs = []
    enhancements = []
    performance = []
    other = []

    # categorize the pull requests based on GitHub labels
    print("Categorizing pull requests", file=sys.stderr)
    for (pull, commit) in all_pulls:

        # see if PR title uses Conventional Commits
        cc_type = ''
        cc_scope = ''
        cc_breaking = ''
        parts = re.findall(r'^([a-z]+)(\([a-z]+\))?(!)?:', pull.title)
        if len(parts) == 1:
            parts_tuple = parts[0]
            cc_type = parts_tuple[0] # fix, feat, docs, chore
            cc_scope = parts_tuple[1] # component within project
            cc_breaking = parts_tuple[2] == '!'

        labels = [label.name for label in pull.labels]
        if 'api change' in labels or cc_breaking:
            breaking.append((pull, commit))
        elif 'performance' in labels or cc_type == 'perf':
            performance.append((pull, commit))
        elif 'bug' in labels or cc_type == 'fix':
            bugs.append((pull, commit))
        elif 'enhancement' in labels or cc_type == 'feat':
            enhancements.append((pull, commit))
        elif 'documentation' in labels or cc_type == 'docs' or cc_type == 'doc':
            docs.append((pull, commit))
        else:
            other.append((pull, commit))

    # produce the changelog content
    print("Generating changelog content", file=sys.stderr)

    # ASF header
    print("""<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->\n""")

    print(f"# Apache DataFusion Ballista {version} Changelog\n")

    # get the number of commits
    commit_count = subprocess.check_output(f"git log --pretty=oneline {tag1}..{tag2} | wc -l", shell=True, text=True).strip()

    # get number of contributors
    contributor_count = subprocess.check_output(f"git shortlog -sn {tag1}..{tag2} | wc -l", shell=True, text=True).strip()

    print(f"This release consists of {commit_count} commits from {contributor_count} contributors. "
          f"See credits at the end of this changelog for more information.\n")

    print_pulls(repo_name, "Breaking changes", breaking)
    print_pulls(repo_name, "Fixed bugs", bugs)
    print_pulls(repo_name, "Performance related", performance)
    print_pulls(repo_name, "Implemented enhancements", enhancements)
    print_pulls(repo_name, "Documentation updates", docs)
    print_pulls(repo_name, "Other", other)

    # show code contributions
    credits = subprocess.check_output(f"git shortlog -sn {tag1}..{tag2}", shell=True, text=True).rstrip()

    print("## Credits\n")
    print("Thank you to everyone who contributed to this release. Here is a breakdown of commits (PRs merged) "
          "per contributor.\n")
    print("```")
    print(credits)
    print("```\n")

    print("Thank you also to everyone who contributed in other ways such as filing issues, reviewing "
          "PRs, and providing feedback on this release.\n")

def resolve_ref(ref):
    """Resolve a git ref (e.g. HEAD, branch name) to a full commit SHA."""
    try:
        return subprocess.check_output(
            ["git", "rev-parse", ref], text=True
        ).strip()
    except subprocess.CalledProcessError:
        # If it can't be resolved locally, return as-is (e.g. a tag name
        # that the GitHub API can resolve)
        return ref


def cli(args=None):
    """Process command line arguments."""
    if not args:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser()
    parser.add_argument("tag1", help="The previous commit or tag (e.g. 52.0.0)")
    parser.add_argument("tag2", help="The current commit or tag (e.g. HEAD)")
    parser.add_argument("version", help="The version number to include in the changelog")
    args = parser.parse_args()

    # Resolve refs to SHAs so the GitHub API compares the same commits
    # as the local git log. Without this, refs like HEAD get resolved by
    # the GitHub API to the default branch instead of the current branch.
    tag1 = resolve_ref(args.tag1)
    tag2 = resolve_ref(args.tag2)

    token = os.getenv("GITHUB_TOKEN")
    project = "apache/datafusion-ballista"

    g = Github(token)
    repo = g.get_repo(project)
    generate_changelog(repo, project, tag1, tag2, args.version)

if __name__ == "__main__":
    cli()
