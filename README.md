# RMQTools

Misc tools for rabbitmq in go

## RMQSender

Send a message on a given channel, with content-type and value.
Depending on value type, it is sent as-it (string) or converted to a bigendian integer (int).



