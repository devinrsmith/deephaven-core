/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.examples;

import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.ObjectService.MessageStream;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.TicketId;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

@Command(name = "interactive-message-stream", mixinStandardHelpOptions = true,
        description = "Interactive message stream", version = "0.1.0")
class InteractiveMessageStream extends SingleSessionExampleBase {

    @Option(names = {"--type"}, required = true, description = "The ticket type.")
    String type;

    @ArgGroup(exclusive = true, multiplicity = "1")
    Ticket ticket;

    @Override
    protected void execute(Session session) throws Exception {
        final AtomicBoolean serverClosed = new AtomicBoolean();
        final MessageStream server = session.messageStream(type, ticket, new MessageStream() {
            @Override
            public void onData(ByteBuffer payload, List<? extends HasTicketId> references) {
                System.out.println("from server:");
                final byte[] dst = new byte[payload.remaining()];
                payload.get(dst);
                System.out.println("payload: " + new String(dst, StandardCharsets.UTF_8));
                System.out.println("num references: " + references.size());
                for (HasTicketId reference : references) {
                    System.out.println("reference: " + reference);
                }
            }

            @Override
            public void onClose() {
                System.out.println("from server: onClose");
                serverClosed.set(true);
            }
        });

        System.out.println(
                "REPL mode. To execute the current script, enter Ctrl+d. To exit, execute an empty script.");
        while (true) {
            System.out.println();
            System.out.print("payload >>> ");
            System.out.flush();
            final String payload = standardInput("payload >>> ", serverClosed);
            if ("exit".equals(payload.trim()) || serverClosed.get()) {
                server.onClose();
                return;
            }
            System.out.println();
            System.out.print("reference >>> ");
            System.out.flush();
            final List<HasTicketId> references = new ArrayList<>();
            while (true) {
                final String reference = standardInput("reference >>> ", serverClosed);
                if ("exit".equals(reference.trim()) || serverClosed.get()) {
                    server.onClose();
                    return;
                }
                if (reference.isEmpty()) {
                    break;
                }
                references.add(new TicketId(reference.trim().getBytes(StandardCharsets.UTF_8)));
            }
            server.onData(ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8)), references);
            // simple wait, allows better printing on server responses to not intermix w/ input
            Thread.sleep(1000);
        }
    }

    private static String standardInput(String lineStart, AtomicBoolean closed) {
        final Scanner scanner =
                new Scanner(new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8)));
        final List<String> lines = new ArrayList<>();
        while (scanner.hasNextLine() && !closed.get()) {
            lines.add(scanner.nextLine());
            System.out.print(lineStart);
            System.out.flush();
        }
        return String.join(System.lineSeparator(), lines);
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new InteractiveMessageStream()).execute(args);
        System.exit(execute);
    }
}
