package kafkainpractice.client

/**
  * Settings shared by every client that talks to one cluster.
  *
  * @param bootstrapServers
  *   a few brokers to start from; the client discovers the rest
  * @param clientId
  *   shows up in broker logs, quotas and metrics, so make it identify the service
  * @param clientRack
  *   the rack or availability zone this client runs in. Consumers send it to the brokers so they
  *   can fetch from a follower in the same zone (KIP-392), which cuts cross-zone traffic
  * @param security
  *   SSL or SASL properties, e.g. `security.protocol`, `sasl.mechanism`, `sasl.jaas.config`
  */
final case class KafkaConnection(
    bootstrapServers: String,
    clientId: String,
    clientRack: Option[String] = None,
    security: Map[String, String] = Map.empty
) {

  private[client] def properties: Map[String, String] =
    security ++ clientRack.map("client.rack" -> _)

}
