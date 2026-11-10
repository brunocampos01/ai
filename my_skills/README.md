# My skills

Skills for [Claude Code](https://docs.claude.com/en/docs/claude-code/overview). Each skill is a
folder with a `SKILL.md` file.

| Skill | How it runs | What it does | Needs |
| --- | --- | --- | --- |
| [`retro`](retro/SKILL.md) | Only when you type `/retro` | Reviews a coding session and suggests changes to the agent's environment (docs, checks, steering files) so later sessions go better | `writing-for-agents` |
| [`writing-for-agents`](writing-for-agents/SKILL.md) | On its own, when Claude creates or edits a skill, `AGENTS.md` or `CLAUDE.md` | Style guide for documents that agents read | — |

`retro` loads `writing-for-agents` in its first step, so always install both.

## Install

Claude Code finds a skill at `<skills folder>/<name>/SKILL.md`. Pick one location:

- **For you, in every project:** `~/.claude/skills/` (on Windows, `C:\Users\<you>\.claude\skills\`).
- **For one project, shared with everyone who clones it:** `.claude/skills/` at the project root.
  Commit the copied folders.

The commands below install into your user folder. To install into a project instead, replace the
destination with that project's `.claude/skills` folder.

### macOS, Linux or Git Bash

```bash
git clone --depth 1 --filter=blob:none --sparse https://github.com/brunocampos01/ai.git /tmp/ai-skills
git -C /tmp/ai-skills sparse-checkout set my_skills
mkdir -p ~/.claude/skills
cp -R /tmp/ai-skills/my_skills/retro /tmp/ai-skills/my_skills/writing-for-agents ~/.claude/skills/
rm -rf /tmp/ai-skills
```

### Windows (PowerShell)

```powershell
git clone --depth 1 --filter=blob:none --sparse https://github.com/brunocampos01/ai.git "$env:TEMP\ai-skills"
git -C "$env:TEMP\ai-skills" sparse-checkout set my_skills
New-Item -ItemType Directory -Force "$HOME\.claude\skills" | Out-Null
Copy-Item -Recurse -Force "$env:TEMP\ai-skills\my_skills\retro", "$env:TEMP\ai-skills\my_skills\writing-for-agents" "$HOME\.claude\skills\"
Remove-Item -Recurse -Force "$env:TEMP\ai-skills"
```

The sparse clone downloads only `my_skills/`, not the rest of this repository.

## Check that it worked

1. Start a new Claude Code session.
2. Type `/retro`. Claude should start a retrospective of the current session, and load
   `writing-for-agents` as its first step.

## Update

Run the install commands again. They overwrite the installed files with the versions on `main`.

## Remove

Delete the `retro` and `writing-for-agents` folders from the skills folder you installed them into.

## Credits

Both skills are copied from [mattpocock/skills](https://github.com/mattpocock/skills) (v1.3.1)
under the MIT License; see [LICENSE](LICENSE).
