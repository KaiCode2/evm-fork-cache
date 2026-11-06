"""Exact Aero alpha acceptance; publication is checked separately from PR packaging."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = 'evm-fork-cache-0.4.2-alpha.1'
REPO = 'KaiCode2/evm-fork-cache'
PR = 21
BASE = '9f0b8b434c1ee4bde4b903c71319bc3899a174cf'
LOCK = '8eccd3a13ee7d9137b66dd3dfe8dc83f9dc57b0bf4c28b43a13cf5317f9a1adc'
PAYLOAD = 'e01a341cad9f30bf05b824b1ece8d19e02ebe2948daceb43e5f5789b776b08d5'
ARCHIVE = 'dac4bf1f5f0d35b24d842e00288b4cc87aff9bed88ae6ba25adfa205626bf89f'
WORKFLOW = 'cef28d25cddce81f00a83b8ac18271cecb74eca6e6e3c9d193ed56346699b4cc'
SOURCE = 'registry+https://github.com/rust-lang/crates.io-index'
BIGINT = ('RUSTSEC-2026-0387', 'crypto-bigint', '0.5.5', SOURCE,
          '0dc92fb57ca44df6db8059111ab3af99a63d5d0f8375d9972e319a379c6bab76')
START = dt.datetime(2026, 11, 6, 11, 28, 47, tzinfo=dt.timezone.utc)
END = dt.datetime(2026, 11, 8, 16, tzinfo=dt.timezone.utc)
POLICY_FILES = {'.github/workflows/ci.yml', '.github/security/aero_alpha_audit.py',
                '.github/security/test_aero_alpha_audit.py', '.github/security/aero-alpha-approval.md'}


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def verify_payload(archive, head, publication=False):
    files = {}
    provenance = None
    with tarfile.open(archive, 'r:gz') as tar:
        for member in tar:
            root, sep, name = member.name.partition('/')
            require(member.isfile() and sep and root == PACKAGE and name
                    and '..' not in Path(name).parts and name not in files,
                    'unexpected or duplicate package member')
            data = tar.extractfile(member).read()
            if name == '.cargo_vcs_info.json':
                require(provenance is None, 'duplicate VCS provenance')
                provenance = json.loads(data)
            else:
                files[name] = [member.mode, sha(data)]
    require(provenance == {'git': {'sha1': head}, 'path_in_vcs': ''}, 'unexpected package provenance')
    require(sha(json.dumps(files, sort_keys=True, separators=(',', ':')).encode()) == PAYLOAD,
            'packaged payload differs from approved alpha')
    if publication:
        require(head == BASE and sha(archive.read_bytes()) == ARCHIVE,
                'publication requires the exact approved source and archive bytes')


def findings(report, inactive, returncode):
    require(returncode == 1 and not report.get('error'), 'unexpected audit result')
    settings = report['settings']
    require(not any(settings[field] for field in ('ignore', 'target_arch', 'target_os', 'severity')),
            'audit report was filtered')
    require({'unmaintained', 'unsound', 'notice'} <= set(settings['informational_warnings']),
            'audit warning classes were filtered')
    summary = report['vulnerabilities']
    entries = summary['list']
    require(summary['count'] == len(entries) and summary['found'] == bool(entries), 'incomplete audit findings')
    rows = [(v['advisory']['id'], *(v['package'][key] for key in ('name', 'version', 'source', 'checksum')))
            for v in entries]
    require(len(rows) == 2 and set(rows) == {BIGINT, inactive}, 'unaccepted, absent or duplicate vulnerability')
    require(re.fullmatch(r'[0-9a-f]{40}', report['database'].get('last-commit') or ''),
            'fresh advisory database identity unavailable')


def qualify(root, now, head, changes, publication=False):
    require(START <= now < END, 'alpha acceptance expired or not yet valid')
    require(not set(changes) - POLICY_FILES, 'source changed outside qualification policy')
    require(sha((root / 'Cargo.lock').read_bytes()) == LOCK, 'lockfile differs from approved alpha')
    if publication:
        require(head == BASE and not changes, 'publication requires approved clean source')
    else:
        require(sha((root / '.github/workflows/ci.yml').read_bytes()) == WORKFLOW,
                'qualification workflow changed')


def github_scope(env, event):
    require(env.get('GITHUB_EVENT_NAME') == 'pull_request'
            and env.get('GITHUB_REPOSITORY') == REPO
            and env.get('GITHUB_REF') == f'refs/pull/{PR}/merge', 'not approved PR context')
    pr = event['pull_request']
    require(event['number'] == PR and pr['base']['ref'] == 'main'
            and pr['base']['repo']['full_name'] == REPO == pr['head']['repo']['full_name'],
            'unapproved PR, repository or promotion')


def run():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--publication-check', action='store_true')
    parser.add_argument('--source-root', type=Path)
    parser.add_argument('--archive', type=Path)
    args = parser.parse_args()
    require(args.publication_check or args.source_root is None, 'source override only for exact publication check')
    require(not args.publication_check or args.source_root is not None, 'explicit publication source required')
    root = (args.source_root or ROOT).resolve()
    if os.environ.get('GITHUB_ACTIONS') == 'true':
        require(not args.publication_check, 'CI acceptance cannot publish')
        github_scope(os.environ, json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text()))
    version = subprocess.check_output(['cargo-audit', '--version'], text=True).strip()
    require(version == 'cargo-audit 0.22.2', 'unexpected cargo-audit version')
    subprocess.run(['git', 'merge-base', '--is-ancestor', BASE, 'HEAD'], cwd=root, check=True)
    subprocess.run(['git', 'diff', '--quiet', 'HEAD', '--'], cwd=root, check=True)
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    changes = list(filter(None, subprocess.check_output(
        ['git', 'diff', '--name-only', '--no-renames', '-z', BASE, 'HEAD'], cwd=root).decode().split('\0')))
    qualify(root, dt.datetime.now(dt.timezone.utc), head, changes, args.publication_check)
    archive = args.archive or root / 'target/package' / (PACKAGE + '.crate')
    verify_payload(archive, head, args.publication_check)
    subprocess.run(['bash', 'scripts/check-security-exceptions.sh'], cwd=root, check=True)
    # Separate cwd/database keeps project ignores and unrelated local entries out.
    with tempfile.TemporaryDirectory() as work:
        result = subprocess.run(['cargo-audit', 'audit', '--db', str(Path(work) / 'advisory-db'),
                                 '--json', '--file', str(root / 'Cargo.lock')],
                                cwd=work, capture_output=True, text=True)
    report = json.loads(result.stdout)
    output = root / 'target/aero-alpha-audit.json'
    output.parent.mkdir(exist_ok=True)
    output.write_text(result.stdout)
    if result.stderr:
        print(result.stderr, file=sys.stderr)
    if summary := os.environ.get('GITHUB_STEP_SUMMARY'):
        with Path(summary).open('a') as stream:
            stream.write('\n### Full Aero dependency audit\n\n```json\n' + result.stdout + '\n```\n')
    locked = tomllib.loads((root / 'Cargo.lock').read_text())['package']
    tracing = next(p for p in locked if p['name'] == 'tracing-subscriber' and p['version'] == '0.2.25')
    inactive = ('RUSTSEC-2025-0055', *(tracing[k] for k in ('name', 'version', 'source', 'checksum')))
    findings(report, inactive, result.returncode)
    qualify(root, dt.datetime.now(dt.timezone.utc), head, changes, args.publication_check)
    verify_payload(archive, head, args.publication_check)
    print(f'{PACKAGE}: exact scoped acceptance, expires {END.isoformat()}; archive {sha(archive.read_bytes())}.')
    print('Publication-check passed for approved bytes.' if args.publication_check else
          'PR qualification only. This policy-head archive is not approved for publication.')
    print('No worker publication, production signing, deployment or activation authority.')


if __name__ == '__main__':
    try:
        run()
    except (ValueError, KeyError, TypeError, OSError, StopIteration, tarfile.TarError,
            subprocess.CalledProcessError) as error:
        sys.exit(f'Aero alpha audit refused: {error}')
