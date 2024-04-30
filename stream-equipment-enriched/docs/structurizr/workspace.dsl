workspace {

    model {
        user = person "Mme Michu" "Public user"
        admin = person "Bob Douglas" "Plateform admin"
        group Châlons {
            gmao = softwareSystem "GMAO"
            iot = softwareSystem "Citylinx"
            hypervisors = softwareSystem "Hypervisor"
            nodered = softwareSystem "NodeRED"
            idp = softwareSystem "OAuth2 provider" "Active Directory"
            gmao -> nodered ""
            hypervisors -> nodered ""
            iot -> nodered ""
        }



        provoly = softwareSystem "Provoly" {

            group Dependencies {
                kafka = container "Events" "" "Kafka" "queue"
                itemIotDB = container "Items from IOT DB" "" "Kuzzle" "db"
                geoserver = container "Geoserver"
            }


            group Hypervisor {
                front = container "Hypervisor dashboard" {
                    technology "Angular"
                    tags spa
                    provoly-dashboard = component "provoly-dashboard"
                    chalons-hyperviseur = component "chalons-hyperviseur"
                }
                hypervisorBack = container "Hypervisor service" {
                    technology "Quarkus"
                }
                db = container "Hypervisor database" "Holds : \n -events\n -equipments\n -Services""Postgres" "db"
            }

            datavirt = container "data-virt" {
                technology "Quarkus"
            }

            user -> front "Uses"
            admin -> front "Uses"

            /* front relations */
            provoly-dashboard -> idp "Uses for user authentication and authorisation"
            provoly-dashboard -> geoserver "Reads from"
            provoly-dashboard -> datavirt "Reads from"
            chalons-hyperviseur -> hypervisorBack "Reads from and Writes to"

            /* back relations */
            hypervisorBack -> idp "Users and systems authentication and authorisation"
            hypervisorBack -> Kafka "Reads from and Writes to" "" "async"
            hypervisorBack -> db "Reads from and Writes to"
            hypervisorBack -> itemIotDB "Reads from and Writes to"
            Kafka -> datavirt "Reads from" "" "async"
            geoserver -> datavirt "Reads from"
            datavirt -> itemIotDB "Reads from"

        }

        nodered -> itemIotDB "Writes to\n" "-equipments\n -Services"
        nodered -> hypervisorBack "Writes to\n" "-equipments\n -Services"
    }

    !script groovy {
        workspace.model.elements.findAll { it instanceof com.structurizr.model.Container }.each { it.addTags(it.technology) }
    }

    views {

        systemlandscape "SystemLandscape" {
            include *
        }

        container provoly all "Just everything" {
            include *
        }

        component front to-and-from-front "To and from ui" {
            include *
        }

        styles {
            element "Element" {
                background #1168bd
                color #ffffff
            }

            relationship "Relationship" {
                style solid
            }

            relationship "async" {
                style dotted
                color #11bdbd
            }

            element "Person" {
                shape Person
            }

            element "spa" {
                shape WebBrowser
            }

            element db {
                shape Cylinder
                 width 350
            }

            element queue {
                shape Pipe
                width 350
            }
        }
    }
}