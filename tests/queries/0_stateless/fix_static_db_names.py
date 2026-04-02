#!/usr/bin/env python3
"""
Fix ClickHouse tests that create databases with static names.

When flaky check runs tests 50 times in parallel, tests that create databases
with static names conflict with each other. Fix by either:
1. Replacing static name with CLICKHOUSE_DATABASE_1 variable (if only 1 extra db)
2. Adding no-flaky-check tag if:
   - Test explicitly checks database names in output (reference file)
   - Test needs 2+ extra databases (CLICKHOUSE_DATABASE_2+ not defined in clickhouse-test)
   - Database name contains special characters (backtick-quoted)

Note: clickhouse-test defines only CLICKHOUSE_DATABASE and CLICKHOUSE_DATABASE_1.

Usage:
    python3 fix_static_db_names.py [--dry-run] [--verbose]
"""

import argparse
import re
from pathlib import Path


TESTS_DIR = Path(__file__).parent


def find_static_db_names_sql(content: str) -> list[str]:
    """Extract static (non-variable) database names from SQL content."""
    names = []
    seen = set()
    # Match CREATE DATABASE [IF NOT EXISTS] name  (handles backtick-quoted names too)
    for match in re.finditer(
        r'\bCREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?(`[^`]+`|\S+)',
        content,
        re.IGNORECASE,
    ):
        name = match.group(1)
        # Strip trailing semicolon for unquoted names
        if not name.startswith('`'):
            name = name.rstrip(';').rstrip()
        # Skip variable references
        if name.startswith('{') or name.upper() == 'IF':
            continue
        if name not in seen:
            seen.add(name)
            names.append(name)
    return names


def find_static_db_names_sh(content: str) -> list[str]:
    """Extract static database names from shell test content."""
    names = []
    seen = set()
    # Strip comment lines first to avoid matching "# Create database and table"
    stripped = re.sub(r'^\s*#.*$', '', content, flags=re.MULTILINE)
    # Match CREATE DATABASE [IF NOT EXISTS] name in SQL strings within shell scripts
    for match in re.finditer(
        r'\bCREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\S+)',
        stripped,
        re.IGNORECASE,
    ):
        name = match.group(1).rstrip('";\'\\').rstrip()
        # Skip variable references ($VAR, ${VAR}, names containing $, {CLICKHOUSE_DATABASE...})
        if '$' in name or name.startswith('{') or name.upper() == 'IF':
            continue
        # Keep names with backticks or special chars — handled as special below

        if name not in seen:
            seen.add(name)
            names.append(name)
    return names


def has_tag(content: str, tag: str, is_sql: bool) -> bool:
    """Check if test already has a given tag."""
    if is_sql:
        return bool(re.search(r'--\s*Tags:.*\b' + re.escape(tag) + r'\b', content))
    else:
        return bool(re.search(r'#\s*Tags:.*\b' + re.escape(tag) + r'\b', content))


def add_tag_sql(content: str, tag: str) -> str:
    """Add a tag to SQL test."""
    tags_match = re.search(r'^(--\s*Tags:\s*)(.*)', content, re.MULTILINE)
    if tags_match:
        existing_tags = tags_match.group(2).strip()
        new_line = tags_match.group(1) + existing_tags + ', ' + tag
        return content[:tags_match.start()] + new_line + content[tags_match.end():]
    else:
        return '-- Tags: ' + tag + '\n' + content


def add_tag_sh(content: str, tag: str) -> str:
    """Add a tag to shell test."""
    tags_match = re.search(r'^(#\s*Tags:\s*)(.*)', content, re.MULTILINE)
    if tags_match:
        existing_tags = tags_match.group(2).strip()
        new_line = tags_match.group(1) + existing_tags + ', ' + tag
        return content[:tags_match.start()] + new_line + content[tags_match.end():]
    else:
        shebang_match = re.match(r'(#!.*\n)', content)
        if shebang_match:
            insert_pos = shebang_match.end()
            return content[:insert_pos] + '# Tags: ' + tag + '\n' + content[insert_pos:]
        else:
            return '# Tags: ' + tag + '\n' + content


def replace_db_name_sql(content: str, old_name: str, new_var: str) -> str:
    """Replace all occurrences of a static database name in SQL content."""
    result = content
    id_sub = '{' + new_var + ':Identifier}'
    str_sub = '{' + new_var + '}'

    # Handle backtick-quoted names specially
    if old_name.startswith('`'):
        # Backtick-quoted: replace as-is in identifier positions
        escaped = re.escape(old_name)
        result = re.sub(escaped + r'(\s*\.)', id_sub + r'\1', result)
        result = re.sub(
            r'((?:CREATE|DROP)\s+DATABASE\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?)' + escaped + r'\b',
            lambda m: m.group(1) + id_sub,
            result,
            flags=re.IGNORECASE,
        )
        result = re.sub(
            r'(USE\s+)' + escaped + r'\b',
            lambda m: m.group(1) + id_sub,
            result,
            flags=re.IGNORECASE,
        )
        return result

    esc = re.escape(old_name)

    # 1. CREATE DATABASE [IF NOT EXISTS] old_name
    result = re.sub(
        r'(CREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?)' + esc + r'\b',
        lambda m: m.group(1) + id_sub,
        result,
        flags=re.IGNORECASE,
    )

    # 2. DROP DATABASE [IF EXISTS] old_name
    result = re.sub(
        r'(DROP\s+DATABASE\s+(?:IF\s+EXISTS\s+)?)' + esc + r'\b',
        lambda m: m.group(1) + id_sub,
        result,
        flags=re.IGNORECASE,
    )

    # 3. USE old_name
    result = re.sub(
        r'(USE\s+)' + esc + r'\b',
        lambda m: m.group(1) + id_sub,
        result,
        flags=re.IGNORECASE,
    )

    # 4. old_name.table (identifier qualifier)
    result = re.sub(
        r'\b' + esc + r'(\s*\.\s*)',
        id_sub + r'\1',
        result,
    )

    # 5. String literals: 'old_name' or "old_name"
    result = re.sub(r"'" + esc + r"'", "'" + str_sub + "'", result)
    result = re.sub(r'"' + esc + r'"', '"' + str_sub + '"', result)

    # 6. RENAME DATABASE old_name TO ...
    result = re.sub(
        r'(RENAME\s+DATABASE\s+)' + esc + r'\b',
        lambda m: m.group(1) + id_sub,
        result,
        flags=re.IGNORECASE,
    )

    # 7. SHOW TABLES FROM old_name / SHOW CREATE DATABASE old_name
    result = re.sub(
        r'((?:SHOW\s+(?:TABLES|CREATE\s+DATABASE)|FROM)\s+)' + esc + r'\b',
        lambda m: m.group(1) + id_sub,
        result,
        flags=re.IGNORECASE,
    )

    return result


def replace_db_name_sh(content: str, old_name: str, new_var: str) -> str:
    """Replace all occurrences of a static database name in shell test content."""
    result = content
    bash_var = '${' + new_var + '}'
    esc = re.escape(old_name)

    result = re.sub(
        r'(CREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?)' + esc + r'\b',
        lambda m: m.group(1) + bash_var,
        result,
        flags=re.IGNORECASE,
    )
    result = re.sub(
        r'(DROP\s+DATABASE\s+(?:IF\s+EXISTS\s+)?)' + esc + r'\b',
        lambda m: m.group(1) + bash_var,
        result,
        flags=re.IGNORECASE,
    )
    result = re.sub(
        r'(USE\s+)' + esc + r'\b',
        lambda m: m.group(1) + bash_var,
        result,
        flags=re.IGNORECASE,
    )
    result = re.sub(r'\b' + esc + r'(\s*\.\s*)', bash_var + r'\1', result)
    result = re.sub(r"'" + esc + r"'", "'" + bash_var + "'", result)
    result = re.sub(r'"' + esc + r'"', '"' + bash_var + '"', result)

    return result


def db_name_in_reference(ref_path: Path, db_names: list[str]) -> list[str]:
    """Check which database names appear in the reference file."""
    if not ref_path.exists():
        return []
    ref_content = ref_path.read_text(encoding='utf-8', errors='replace')
    result = []
    for name in db_names:
        # For backtick-quoted names, check the unquoted form
        check_name = name.strip('`') if name.startswith('`') else name
        if re.search(r'\b' + re.escape(check_name) + r'\b', ref_content):
            result.append(name)
    return result


def has_special_chars(name: str) -> bool:
    """Check if database name has special characters (backtick-quoted)."""
    return name.startswith('`')


def process_sql_file(sql_path: Path, dry_run: bool, verbose: bool = False) -> tuple[str, str]:  # noqa: ARG001
    """Process a SQL test file. Returns (action, description)."""
    content = sql_path.read_text(encoding='utf-8', errors='replace')

    if has_tag(content, 'no-flaky-check', is_sql=True):
        return 'skip', 'already has no-flaky-check'

    db_names = find_static_db_names_sql(content)
    if not db_names:
        return 'skip', 'no static CREATE DATABASE'

    ref_path = sql_path.with_suffix('.reference')
    names_in_ref = db_name_in_reference(ref_path, db_names)

    # Reasons to add no-flaky-check instead of rewriting:
    reason = None
    if names_in_ref:
        reason = f'db names in reference: {names_in_ref}'
    elif any(has_special_chars(n) for n in db_names):
        reason = f'special-char db names: {[n for n in db_names if has_special_chars(n)]}'
    elif len(db_names) > 1:
        reason = f'needs {len(db_names)} extra databases (only CLICKHOUSE_DATABASE_1 is defined)'

    if reason:
        new_content = add_tag_sql(content, 'no-flaky-check')
        if not dry_run:
            sql_path.write_text(new_content, encoding='utf-8')
        return 'no_flaky_check', reason

    # Single extra database: rewrite to CLICKHOUSE_DATABASE_1
    assert len(db_names) == 1
    name = db_names[0]
    var = 'CLICKHOUSE_DATABASE_1'
    new_content = replace_db_name_sql(content, name, var)

    if new_content == content:
        return 'skip', f'no changes after rewrite attempt (name: {name})'

    if not dry_run:
        sql_path.write_text(new_content, encoding='utf-8')

    return 'rewrite', f'{name} -> {{{var}:Identifier}}'


def process_sh_file(sh_path: Path, dry_run: bool, verbose: bool = False) -> tuple[str, str]:  # noqa: ARG001
    """Process a shell test file."""
    content = sh_path.read_text(encoding='utf-8', errors='replace')

    if has_tag(content, 'no-flaky-check', is_sql=False):
        return 'skip', 'already has no-flaky-check'

    db_names = find_static_db_names_sh(content)
    if not db_names:
        return 'skip', 'no static CREATE DATABASE'

    ref_path = sh_path.with_suffix('.reference')
    names_in_ref = db_name_in_reference(ref_path, db_names)

    reason = None
    if names_in_ref:
        reason = f'db names in reference: {names_in_ref}'
    elif any('`' in n for n in db_names):
        reason = f'special-char db names: {[n for n in db_names if "`" in n]}'
    elif len(db_names) > 1:
        reason = f'needs {len(db_names)} extra databases'

    if reason:
        new_content = add_tag_sh(content, 'no-flaky-check')
        if not dry_run:
            sh_path.write_text(new_content, encoding='utf-8')
        return 'no_flaky_check', reason

    assert len(db_names) == 1
    name = db_names[0]
    var = 'CLICKHOUSE_DATABASE_1'
    new_content = replace_db_name_sh(content, name, var)

    if new_content == content:
        return 'skip', f'no changes after rewrite attempt (name: {name})'

    if not dry_run:
        sh_path.write_text(new_content, encoding='utf-8')

    return 'rewrite', f'{name} -> ${{{var}}}'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    parser.add_argument('--verbose', action='store_true', help='Show all files including skipped ones')
    parser.add_argument('--sql-only', action='store_true', help='Only process .sql files')
    parser.add_argument('--sh-only', action='store_true', help='Only process .sh files')
    parser.add_argument('files', nargs='*', help='Specific files to process (default: all in tests/queries/0_stateless)')
    args = parser.parse_args()

    if args.files:
        all_files = [Path(f) for f in args.files]
    else:
        all_files = sorted(TESTS_DIR.glob('*.sql')) + sorted(TESTS_DIR.glob('*.sh'))

    counts: dict[str, int] = {}

    for path in all_files:
        if path.suffix == '.sql' and not args.sh_only:
            action, desc = process_sql_file(path, args.dry_run, args.verbose)
        elif path.suffix == '.sh' and not args.sql_only:
            action, desc = process_sh_file(path, args.dry_run, args.verbose)
        else:
            continue

        counts[action] = counts.get(action, 0) + 1

        if action != 'skip' or args.verbose:
            prefix = '[DRY-RUN] ' if args.dry_run else ''
            print(f'{prefix}{action.upper()}: {path.name}: {desc}')

    print()
    print(f'Summary: {counts.get("rewrite", 0)} rewritten, {counts.get("no_flaky_check", 0)} tagged no-flaky-check, {counts.get("skip", 0)} skipped, {counts.get("error", 0)} errors')


if __name__ == '__main__':
    main()
