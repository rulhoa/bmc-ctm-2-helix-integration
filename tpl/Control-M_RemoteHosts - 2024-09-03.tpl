// Copyright (c) 2020 BMC Software, Inc.
// All rights reserved.

tpl 1.19 module ControlM_RH;

metadata
    origin    := 'TKU';
    tkn_name  := 'Control-M';
    tree_path := 'System and Network Management Software', 'Workload Scheduling and Automation Software', 'BMC CONTROL-M';
end metadata;

from SearchFunctions import SearchFunctions 1.5;

definitions apiFunctions 1.0
    '''
        Control-M API functions

        api_call() - Makes GET query to the REST API on the 8443 port.
                     Returns decoded body, if the response is good.
    '''

    type := function;

    define api_call(host, request) -> decoded
    '''
        runs the request
    '''
        decoded := [];
        result  := discovery.restfulGet(
            host,
            'controlm_auth',
            request,
            port := 8443
        );
        if result and result.response_status = 200 then
            decoded := json.decode(result.response_body);
            if not decoded then
                return [];
            end if;
        end if;
        return decoded;
    end define;

end definitions;

pattern ControlM_Remote_Hosts 1.0
    '''
        API is the part of the Control-M Enterprise Manager,
        so, pattern triggers on its SI.
        Then, via the API, the pattern queries for Control-M
        servers, Remote Hosts and agents, to which that Remote
        Hosts are connected.
        After that it creates Management links between agents
        and Remote Hosts.
    '''

    metadata
        publishers := 'BMC';
        products   := 'Control-M';
        categories := 'Workload Scheduling and Automation Software';
        urls       := 'https://www.bmc.com/it-solutions/control-m.html';
    end metadata;

    overview
        tags TKU, TKU_2020_04_01, restful, ControlM;
    end overview;

    constants
        type := 'Control-M Remote Host';
    end constants;

    triggers
        on em_si := SoftwareInstance created, confirmed
            where type = 'Control-M/Enterprise Manager Server';
    end triggers;

    body
        host             := model.host(em_si);
        servers          := [];
        server_hostnames := [];

        servers_decoded := apiFunctions.api_call(
            host,
            '/automation-api/config/servers'
        );

        if servers_decoded then
            // Example: [{"name":"ctm2","host":"ctm2","state":"Up","message":"Connected"},{}]
            for server in servers_decoded do
                if server.name then
                    list.append(servers, server.name);
                end if;
                if server.host then
                    list.append(server_hostnames, server.host);
                end if;
            end for;
        else
            log.info('Failed to run API call. Stopping');
            model.suppressRemovalGroup('EM_API');
            stop;
        end if;

        if server_hostnames then
            server_sis := SearchFunctions.relatedSisSearchOnMultipleHosts(
                host,
                server_hostnames,
                'Control-M/Server'
            );

            server_si_keys := [];
            for server_si in server_sis do
                list.append(server_si_keys, server_si.key);
                rel := model.rel.Communication(
                    Server := em_si,
                    Client := server_si
                );
                model.setRemovalGroup(rel, 'EM_API');
            end for;

            if server_si_keys then
                old_server_links := search (
                    in em_si
                    step in Server:Communication
                        where #:Client:SoftwareInstance.type = 'Control-M/Server'
                            and not #:Client:SoftwareInstance.key_group
                            and not #:Client:SoftwareInstance.key in %server_si_keys%
                );
                if old_server_links then
                    model.destroy(old_server_links);
                end if;
            end if;
        end if;

        for server in servers do
            // Example: ["ctm4", "ctm5"]
            remote_hosts := apiFunctions.api_call(
                host,
                '/automation-api/config/server/%server%/remotehosts'
            );

            if not remote_hosts then
                remote_hosts := apiFunctions.api_call(
                    host,'/automation-api/config/server/%server%/agentlesshosts');
            end if;

            if not remote_hosts then
                continue;
            end if;

            for remote_hostname in remote_hosts do
                // Example: ["rh11", "rh12"]
                agents := [];
                remote_host_conf := apiFunctions.api_call(
                    host,
                    '/automation-api/config/server/%server%/remotehost/%remote_hostname%'
                );
                // Example: [
                // {
                //     "port":22,
                //     "encryptAlgorithm":"BLOWFISH",
                //     "compression":false,
                //     "agents":[
                //         "<Local>",
                //         "controlm2",
                //         "ctm1",
                //         "ctm3"
                //     ]
                // }, {}]

                if not remote_host_conf then
                    // /config/server/{server}/agentlesshost/{agentlesshost}
                    remote_host_conf := apiFunctions.api_call(
                        host,
                        '/automation-api/config/server/%server%/agentlesshost/%remote_hostname%'
                    );
                end if;

                if remote_host_conf and remote_host_conf.agents then
                    if '<Local>' in remote_host_conf.agents and not host.hostname in remote_host_conf.agents then
                        for agent_hostname in remote_host_conf.agents do
                            if agent_hostname = '<Local>' then
                                list.append(agents, server);
                            else
                                list.append(agents, agent_hostname);
                            end if;
                        end for;
                    else
                        agents := remote_host_conf.agents;
                    end if;
                else
                    continue;
                end if;

                agent_sis := SearchFunctions.relatedSisSearchOnMultipleHosts(
                    host,
                    agents,
                    'Control-M/Agent Listener'
                );

                if not agent_sis then
                    continue;
                end if;

                remote_host, _ := SearchFunctions.getHostingNodes(
                    host,
                    remote_hostname
                );

                if not remote_host then
                    continue;
                end if;

                rel := model.rel.Management(
                    Manager        := agent_sis,
                    ManagedElement := remote_host
                );
                model.setRemovalGroup(rel, 'EM_API');
            end for;
        end for;

    end body;
end pattern;

