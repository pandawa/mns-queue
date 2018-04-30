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

namespace Pandawa\Mns\Queue;

use Aliyun\MNS\Exception\MessageNotExistException;
use Aliyun\MNS\Requests\SendMessageRequest;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Pandawa\Mns\Adapter\MnsAdapter;
use Pandawa\Mns\Exception\MnsQueueException;
use Pandawa\Mns\Job\MnsJob;

/**
 * @author  Iqbal Maulana <iq.bluejack@gmail.com>
 */
final class MnsQueue extends Queue implements QueueContract
{
    /**
     * @var MnsAdapter
     */
    private $adapter;

    /**
     * @var string
     */
    private $default;

    /**
     * @var int
     */
    private $waitSeconds;

    /**
     * Constructor.
     *
     * @param MnsAdapter $adapter
     * @param string     $default
     * @param int        $waitSeconds
     */
    public function __construct(MnsAdapter $adapter, string $default, int $waitSeconds = null)
    {
        $this->adapter = $adapter;
        $this->default = $default;
        $this->waitSeconds = $waitSeconds;
    }


    /**
     * {@inheritdoc}
     * @throws MnsQueueException
     */
    public function size($queue = null)
    {
        throw new MnsQueueException('The size method is not support for aliyun-mns');
    }

    /**
     * {@inheritdoc}
     */
    public function push($job, $data = '', $queue = null)
    {
        $payload = $this->createPayload($job, $data);

        return $this->pushRaw($payload, $queue);
    }

    /**
     * {@inheritdoc}
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $message = new SendMessageRequest($payload);

        return $this->adapter->sendMessage($this->getQueue($queue), $message)->getMessageId();
    }

    /**
     * {@inheritdoc}
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        $seconds = $this->secondsUntil($delay);
        $payload = $this->createPayload($job, $data);
        $message = new SendMessageRequest($payload, $seconds);

        return $this->adapter->sendMessage($this->getQueue($queue), $message)->getMessageId();
    }

    /**
     * {@inheritdoc}
     */
    public function pop($queue = null)
    {
        $queue = $this->getDefaultIfNull($queue);

        try {
            $response = $this->adapter->receiveMessage($this->getQueue($queue), $this->waitSeconds);
        } catch (MessageNotExistException $e) {
            $response = null;
        }

        if ($response) {
            return new MnsJob($this->container, $this->adapter, $queue, $response);
        }

        return null;
    }

    /**
     * @param string|null $queue
     *
     * @return string
     */
    public function getQueue(?string $queue): string
    {
        return $queue ?: $this->default;
    }

    /**
     * @param $wanted
     *
     * @return string
     */
    private function getDefaultIfNull(?string $wanted): string
    {
        return $wanted ? $wanted : $this->default;
    }
}