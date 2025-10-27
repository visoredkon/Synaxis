from loguru import logger

from . import ConsensusMessage, ConsensusProtocol, ConsensusState


class PBFTConsensus(ConsensusProtocol):
    view_number: int
    sequence_number: int
    primary_id: str | None

    def __init__(self, node_id: str, cluster_nodes: list[str]) -> None:
        super().__init__(node_id, cluster_nodes)
        self.view_number = 0
        self.sequence_number = 0
        self.primary_id = None

    async def start(self) -> None:
        self.state = ConsensusState.FOLLOWER
        self._calculate_primary()
        logger.info(f"PBFT consensus started for node {self.node_id}")

    async def stop(self) -> None:
        logger.info(f"PBFT consensus stopped for node {self.node_id}")

    async def propose(self, data: object) -> bool:
        if not self.is_leader():
            logger.warning(f"Node {self.node_id} is not primary, cannot propose")
            return False
        logger.info("PBFT propose not fully implemented yet")
        return False

    async def handle_message(
        self, message: ConsensusMessage
    ) -> ConsensusMessage | None:
        logger.debug("PBFT message handling not fully implemented yet")
        return None

    def is_leader(self) -> bool:
        return self.primary_id == self.node_id

    def get_leader_id(self) -> str | None:
        return self.primary_id

    def _calculate_primary(self) -> None:
        if self.cluster_nodes:
            primary_index = self.view_number % len(self.cluster_nodes)
            self.primary_id = sorted(self.cluster_nodes)[primary_index]
            logger.debug(f"Primary for view {self.view_number} is {self.primary_id}")


__all__ = ["PBFTConsensus"]
