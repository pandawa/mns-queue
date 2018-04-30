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

namespace Pandawa\Mns\Job;

use Aliyun\MNS\Responses\ReceiveMessageResponse;
use Illuminate\Contracts\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job;
use Pandawa\Mns\Adapter\MnsAdapter;
use Pandawa\Mns\Exception\MnsQueueException;

/**
 * @author  Iqbal Maulana <iq.bluejack@gmail.com>
 */
final class MnsJob extends Job implements JobContract
{
    /**
     * @var MnsAdapter
     */
    private $adapter;

    /**
     * @var ReceiveMessageResponse
     */
    private $job;

    /**
     * Constructor.
     *
     * @param Container              $container
     * @param MnsAdapter             $mns
     * @param string                 $queue
     * @param ReceiveMessageResponse $job
     */
    public function __construct(Container $container, MNSAdapter $mns, string $queue, ReceiveMessageResponse $job)
    {
        $this->container = $container;
        $this->adapter = $mns;
        $this->queue = $queue;
        $this->job = $job;
    }

    /**
     * {@inheritdoc}
     */
    public function getJobId()
    {
        return $this->job->getMessageId();
    }

    /**
     * {@inheritdoc}
     */
    public function getRawBody(): string
    {
        return $this->job->getMessageBody();
    }

    /**
     * {@inheritdoc}
     */
    public function attempts(): int
    {
        return (int)$this->job->getDequeueCount();
    }

    /**
     * @throws MnsQueueException
     */
    public function fire(): void
    {
        if (method_exists($this, 'resolveAndFire')) {
            $payload = json_decode($this->getRawBody(), true);
            if (!is_array($payload)) {
                throw new MnsQueueException("Seems it's not a Laravel enqueued job. [$payload]");
            }

            $this->resolveAndFire($payload);

            return;
        }

        parent::fire();
    }

    public function delete(): void
    {
        parent::delete();
        $this->adapter->deleteMessage($this->queue, $this->job->getReceiptHandle());
    }

    /**
     * @param int $delay
     */
    public function release($delay = 0): void
    {
        parent::release($delay);

        if ($delay < 1) {
            $delay = 1;
        }

        $this->adapter->changeMessageVisibility($this->queue, $delay, $this->job->getReceiptHandle());
    }
}