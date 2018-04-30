<?php
/**
 * This file is part of the Pandawa Mns Queue package.
 *
 * (c) 2018 Pandawa <https://github.com/pandawa/pandawa>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace Pandawa\Mns\Adapter;

use Aliyun\MNS\Client;
use Aliyun\MNS\Queue;
use Aliyun\MNS\Requests\SendMessageRequest;
use Aliyun\MNS\Responses\ReceiveMessageResponse;
use Aliyun\MNS\Responses\SendMessageResponse;

/**
 * @author  Iqbal Maulana <iq.bluejack@gmail.com>
 */
final class MnsAdapter
{
    /**
     * @var Client
     */
    private $client;

    /**
     * @var Queue[]
     */
    private $queues = [];

    /**
     * Constructor.
     *
     * @param Client $client
     */
    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    /**
     * @param string             $queue
     * @param SendMessageRequest $message
     *
     * @return SendMessageResponse
     */
    public function sendMessage(string $queue, SendMessageRequest $message): SendMessageResponse
    {
        return $this->onQueue($queue)->sendMessage($message);
    }

    /**
     * @param string   $queue
     * @param int|null $waitSeconds
     *
     * @return ReceiveMessageResponse
     */
    public function receiveMessage(string $queue, int $waitSeconds = null): ReceiveMessageResponse
    {
        return $this->onQueue($queue)->receiveMessage($waitSeconds);
    }

    /**
     * @param string $queue
     * @param mixed  $receiptHandle
     */
    public function deleteMessage(string $queue, $receiptHandle): void
    {
        $this->onQueue($queue)->deleteMessage($receiptHandle);
    }

    /**
     * @param string $queue
     * @param int    $delay
     * @param mixed  $receiptHandle
     */
    public function changeMessageVisibility(string $queue, int $delay, $receiptHandle): void
    {
        $this->onQueue($queue)->changeMessageVisibility($receiptHandle, $delay);
    }

    /**
     * @param string $queue
     *
     * @return Queue
     */
    private function onQueue(string $queue): Queue
    {
        if (!isset($this->queues[$queue])) {
            $this->queues[$queue] = $this->client->getQueueRef($queue);
        }

        return $this->queues[$queue];
    }
}