## Repo-local rules

These are the broader elevated tool approvals we’ve established in this session, with the exact binary paths to use:
rg
C:\Users\Ryzen-pc\Desktop\sys-dds\codex-scratch\ripgrep-15.1.0-x86_64-pc-windows-msvc\rg.exe

fd: C:\Users\Ryzen-pc\Desktop\sys-dds\codex-scratch\fd-v10.4.2-x86_64-pc-windows-msvc\fd.exe
jq: C:\Users\Ryzen-pc\Desktop\sys-dds\codex-scratch\jq.exe
sd: C:\Users\Ryzen-pc\Desktop\sys-dds\codex-scratch\sd-v1.1.0-x86_64-pc-windows-msvc\sd.exe
delta: C:\Users\Ryzen-pc\Desktop\sys-dds\codex-scratch\delta-0.19.2-x86_64-pc-windows-msvc\delta.exe
just: C:\Users\Ryzen-pc\Desktop\sys-dds\codex-scratch\just.exe



- Get-Command rg -All
- rg --version
- Use `C:\Users\Ryzen-pc\Desktop\sys-dds\codex-scratch` for temporary files, downloads, extracted tools, logs, caches, and other throwaway execution artifacts.
- Do not leave temporary artifacts in this repo.

## Execution rule
do not run automated tests
do not write new tests
focus on implementing the checklist fully
only do a fresh build/compile to catch obvious breakage