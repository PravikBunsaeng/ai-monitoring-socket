<?php
class Database
{
    private $host = "localhost:3307";
    private $db_name = "ai_online_test_system";
    private $username = "root";
    private $password = "";
    public $conn;

    public function connect()
    {
        $this->conn = null;

        try {
            $this->conn = new mysqli(
                $this->host,
                $this->username,
                $this->password,
                $this->db_name
            );

            if ($this->conn->connect_error) {
                die("Database Connection Failed: " . $this->conn->connect_error);
            }

            // Set charset (important for system stability)
            $this->conn->set_charset("utf8mb4");

        } catch (Exception $e) {
            die("Database Error: " . $e->getMessage());
        }

        return $this->conn;
    }
}
?>
