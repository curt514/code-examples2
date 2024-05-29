<?php
declare(strict_types=1);

namespace PR\RateLimiter;

//usage for block ddos orders
$rLimiter = new RLimiter(123);
if ($rLimiter->checkOrderIsNotBlocked()) { /* some logic */ }

final class RLimiter
{
    private $pdoCounter;

    private $orderId;

    public function __construct(int $orderId)
    {
        $this->pdoCounter = $this->getPdo();
        if (empty($orderId)) {
            $orderId = $this->getOrderIdFromUrl();
        }
        $this->orderId = $orderId;
    }

    private function getOrderIdFromUrl():mixed
    {
        if (!empty($_GET['CustomerNumber'])) {
            $orderId = intval($_GET['CustomerNumber']);
            if (!empty($orderId)) {
                return $orderId;
            }
        }

        return false;
    }

    public function checkOrderIsNotBlocked():bool
    {
        if (!$this->checkIsAllEnvVarsNotEmpty() || !$this->pdoCounter || !$this->orderId) {

            return true;
        }

        if ($this->checkIsOrderNew()) {
            $counterId = $this->insertNewRowForOrderCounter();
            $this->insertUniqueOrderUrl($counterId);

            return true;
        }

        $tryingCounter = $this->getCounterValueForOrder();
        $tryingTillTime = strtotime($this->getTillTimeForOrder());

        if ($tryingCounter == 10) {
            if (time() > $tryingTillTime) {//unblock
                $this->resetCounterForOrder();

                return true;
            }

            return false;
        }

        $this->increaseCounterValueForOrder();
        $currentCounterValue = $this->getCounterValueForOrder();
        if ($currentCounterValue == 10) {
            $this->increaseBlockCounterAndUpdateTillTimeForOrder();

            return true;
        }

        return true;
    }

    private function checkIsAllEnvVarsNotEmpty():bool
    {
        if (empty($_ENV['DB_HOST']) ||
            empty($_ENV['DB_PORT']) ||
            empty($_ENV['DB']) ||
            empty($_ENV['DB_USER']) ||
            empty($_ENV['DB_PASSWORD'])) {

            return false;
        }

        return true;
    }

    private function getPdo():mixed
    {
        try {
            $pdo = new \PDO("mysql:host={$_ENV['DB_HOST']};port={$_ENV['DB_PORT']};dbname={$_ENV['DB']}",
                $_ENV['DB_USER'],
                $_ENV['DB_PASSWORD']);
        }
        catch (\Exception $e) {
            static::writeLogMsg('sbrf', 'ratelimiter', $e->getMessage());

            return false;
        }

        return $pdo;
    }

    //successfully tested on prod

    public static function writeLogMsg(string $directory, string $logNname, string $text):void
    {
        $logDir = "/var/www/vhosts/info/_logs/{$directory}";

        if ( !is_dir($logDir) ) {
            mkdir($logDir);
            chmod($logDir, 0775);
            chgrp($logDir, 'psacln');
        }

        $logDir = "/var/www/vhosts/info/_logs/{$directory}/{$logNname}";

        if ( !is_dir($logDir) ) {
            mkdir($logDir);
            chmod($logDir, 0775);
            chgrp($logDir, 'psacln');
        }

        $logDir = "/var/www/vhosts/info/_logs/{$directory}/{$logNname}/" . date('Ym');

        if ( !is_dir($logDir) ) {
            mkdir($logDir);
            chmod($logDir, 0775);
            chgrp($logDir, 'psacln');
        }

        $logPath = $logDir . '/' . date('Ymd') . "_{$logNname}.log";

        if (is_array($text)) {
            $text = var_export($text, true);
        }

        file_put_contents($logPath, $text . PHP_EOL, FILE_APPEND);
    }

    private function checkIsOrderNew():bool
    {
        $sql = "
    SELECT * FROM count_order_pay WHERE order_id = :order_id
    ";
        $st = $this->pdoCounter->prepare($sql);
        $st->execute([
            'order_id' => $this->orderId,
        ]);
        $result = $st->fetchAll();
        if (empty($result)) {
            return true;
        }

        return false;
    }

    private function insertNewRowForOrderCounter():int
    {
        $createdAt = date('Y-m-d H:i:s', time());
        $sql = "
    INSERT INTO count_order_pay
    SET
        order_id = :order_id, 
        counter = 1, 
        created_at = :created_at
    ";

        $st = $this->pdoCounter->prepare($sql);
        $st->execute([
            'order_id' => $this->orderId,
            'created_at' => $createdAt,
        ]);

        return $this->pdoCounter->lastInsertId();
    }

    private function insertUniqueOrderUrl(int $counterId):void
    {
        $url = '/payonline/?' . http_build_query($_GET);
        $urlHash = md5($url);
        $sql1 = "
    SELECT * FROM count_order_pay_url rt WHERE rt.count_order_pay_id = :counter_id
    ";
        $st = $this->pdoCounter->prepare($sql1);
        $st->execute([
            'counter_id' => $counterId,
        ]);

        if (empty($st->fetchAll())) {
            $sql2 = "
    INSERT INTO count_order_pay_url
    (count_order_pay_id, url_hash, url, created_at)
        VALUES 
    (:counter_id, :url_hash, :url, NOW())
    ";
            $st = $this->pdoCounter->prepare($sql2);
            $st->execute([
                'counter_id' => $counterId,
                'url_hash' => $urlHash,
                'url' => $url,
            ]);
        }
    }

    private function getCounterValueForOrder():mixed
    {
        $sql = "
    SELECT cp.counter FROM count_order_pay cp WHERE order_id = :order_id
    ";
        $st = $this->pdoCounter->prepare($sql);
        $st->execute([
            'order_id' => $this->orderId,
        ]);

        return $st->fetchColumn();
    }

    private function getTillTimeForOrder():mixed
    {
        $sql = "
    SELECT cp.blocked_till FROM count_order_pay cp WHERE order_id = :order_id
    ";
        $st = $this->pdoCounter->prepare($sql);
        $st->execute([
            'order_id' => $this->orderId,
        ]);

        return $st->fetchColumn();
    }

    private function increaseCounterValueForOrder():void
    {
        $sql = "
    UPDATE count_order_pay SET counter = counter + 1, updated_at = NOW() WHERE order_id = :order_id
    ";
        $st = $this->pdoCounter->prepare($sql);
        $st->execute([
            'order_id' => $this->orderId,
        ]);
    }

    private function resetCounterForOrder():void
    {
        $sql = "
    UPDATE count_order_pay SET counter = 1, updated_at = NOW() WHERE order_id = :order_id
    ";
        $st = $this->pdoCounter->prepare($sql);
        $st->execute([
            'order_id' => $this->orderId,
        ]);
    }

    private function increaseBlockCounterAndUpdateTillTimeForOrder():void
    {
        $blockedFrom = date('Y-m-d H:i:s', time());
        $blockedTill = strtotime('+10 minutes');
        $blockedTill = date('Y-m-d H:i:s', $blockedTill);
        $sql = "
    UPDATE count_order_pay 
    SET 
    block_counter = block_counter + 1,
    blocked_from = :blocked_from,
    blocked_till = :blocked_till,
    updated_at = NOW()
    WHERE order_id = :order_id
    ";
        $st = $this->pdoCounter->prepare($sql);
        $st->execute([
            'blocked_from' => $blockedFrom,
            'blocked_till' => $blockedTill,
            'order_id' => $this->orderId,
        ]);
    }
}