"""The workload agent: the small API on the machine the workload runs on.

A fermenter's workload runs at a fake date by setting its machine's system
clock, so everything that must stay on real time (the fermenter server,
builds, the archive, observability) is meant to live elsewhere, on a control
VM (docs/split-vms.md). The agent is what's left next to the workload: it
sets the clock, runs process-compose, and serves what only that machine can
read (process logs, clock probes, the offline database).

    ferment.agent.app     the agent (create_agent_app), run on the workload's machine
    ferment.agent.client  AgentClient: how the fermenter server drives it

The agent keeps no records of its own; the fermenter server decides what runs
and when, and the agent does it.
"""
