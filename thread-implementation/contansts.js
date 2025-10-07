export const ADMIN_COMMAND_TYPES = {
    CREATE_TOPIC: 'create_topic',
    EDIT_PARTITIONS: 'edit_partitions',
    LIST_TOPICS: 'list_topics',
}

export const KAFKA_CLIENTS = {
    ADMIN: 'admin',
    PRODUCER: 'producer',
    CONSUMER: 'consumer',
}

export const PRODUCER_COMMANDS = {
    PRODUCE: 'produce',
}

export const API_TYPES = {
    ...ADMIN_COMMAND_TYPES,
    ...PRODUCER_COMMANDS
}