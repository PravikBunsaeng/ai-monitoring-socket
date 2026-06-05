<?php
/**
 * Copy to realtime_host.php and set your PC's LAN IP so phones/tablets
 * connect to the Socket.IO server on the same network.
 *
 * Example (XAMPP on 192.168.1.10):
 *   'host' => '192.168.1.10',
 *   'port' => 4000,
 */
return [
    'host' => '10.216.138.226',
    'port' => 4000,
    'https' => false,
    // Or set full URL:
    // 'socket_url' => 'http://192.168.1.10:4000',
];
