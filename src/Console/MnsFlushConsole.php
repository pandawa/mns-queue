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

namespace Pandawa\Mns\Console;

use Aliyun\MNS\Client;
use Aliyun\MNS\Model\Message;
use Aliyun\MNS\Requests\BatchReceiveMessageRequest;
use Exception;
use Illuminate\Console\Command;

/**
 * @author  Iqbal Maulana <iq.bluejack@gmail.com>
 */
final class MnsFlushConsole extends Command
{
    /**
     * @var string
     */
    protected $signature = 'queue:mns:flush {queue? : The queue name} {--c|connection=mns : The queue connection}';

    /**
     * @var string
     */
    protected $description = 'Flush MNS Queue';

    public function handle(): void
    {
        $queue = $this->argument('queue');
        $connection = $this->option('connection');
        $config = config(sprintf('queue.connections.%s', $connection));

        if (!$queue) {
            $queue = $config['queue'];
        }

        $client = new Client($config['endpoint'], $config['key'], $config['secret']);
        $queue = $client->getQueueRef($queue);
        $hasMessage = true;

        while ($hasMessage) {
            $this->info('Peeking messages (Polling...)');

            try {
                $response = $queue->batchPeekMessage(15);
                if ($response->getMessages()) {
                    $hasMessage = true;
                } else {
                    $hasMessage = false;
                }
            } catch (Exception $e) {
                $this->info('no messages');
                break;
            }

            $response = $queue->batchReceiveMessage(new BatchReceiveMessageRequest(15, 30));
            $handles = [];

            /** @var Message $message */
            foreach ($response->getMessages() as $message) {
                $handles[] = $message->getReceiptHandle();
            }

            $response = $queue->batchDeleteMessage($handles);

            if ($response->isSucceed()) {
                foreach ($handles as $handle) {
                    $this->info(sprintf("The message: %s deleted success", $handle));
                }
            }
        }
    }
}