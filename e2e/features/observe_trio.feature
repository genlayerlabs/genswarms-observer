Feature: The trio end-to-end — a swarm builds, serves its dashboard, the observer sees it
  The real production loop, all three components live: a target swarm is built
  from config (IR), a GenswarmsDashboard.Objects.Dashboard object stands up its
  HTTP endpoint, and the observer reads that endpoint over :httpc (Client.Http)
  and runs its detectors. No fakes — the observer fetches a real dashboard.

  Scenario: the observer fetches a live swarm's real dashboard envelope
    Given a target swarm built from config with a dashboard object on a port
    When the observer fetches that swarm's dashboard over HTTP
    Then it gets a real envelope naming the swarm and its nodes

  Scenario: the detectors stay quiet on the freshly-built healthy swarm
    Given a target swarm built from config with a dashboard object on a port
    When the observer fetches the dashboard and runs its detectors
    Then no alert is raised for a healthy swarm

  Scenario: the observer raises endpoint_down when the swarm's dashboard dies
    Given a target swarm built from config with a dashboard object on a port
    When the swarm is stopped and the observer fetches again
    Then the fetch fails and the detectors raise endpoint_down
